import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3 = new S3Client({ region: process.env.AWS_REGION || "eu-west-1" });
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || "britbox-epg-schedule-p7-output-stage";
const INPUT_BUCKET = process.env.INPUT_BUCKET || "britbox-epg-schedule-p7-stage";
const DEFAULT_BLACKOUT_DAYS = parseInt(process.env.BLACKOUT_DAYS || "30", 10);
const SIGNED_URL_EXPIRY = parseInt(process.env.SIGNED_URL_EXPIRY || "86400", 10);

const CHANNEL_MAP = {
  First: "BBAF",
  Entertain: "BBAE",
  Select: "BBAD",
};

const FILE_REGEX = /^bbi_BBAU BBC (\w+)_(\d{8})_V(\d+)_axis\.xml$/;
const BATCH_SIZE = 20;

/**
 * Parse DDMMYYYY string to a Date (UTC midnight).
 */
function parseDateFromFilename(ddmmyyyy) {
  const day = parseInt(ddmmyyyy.slice(0, 2), 10);
  const month = parseInt(ddmmyyyy.slice(2, 4), 10) - 1;
  const year = parseInt(ddmmyyyy.slice(4, 8), 10);
  return new Date(Date.UTC(year, month, day));
}

/**
 * Format a Date as YYYY-MM-DD.
 */
function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

/**
 * Get today (UTC midnight) and end date based on days window.
 */
function getDateRange(days) {
  const today = new Date();
  today.setUTCHours(0, 0, 0, 0);
  const end = new Date(today);
  end.setUTCDate(end.getUTCDate() + days);
  return { from: today, to: end };
}

/**
 * List all objects in the output bucket, handling pagination.
 */
async function listAllObjects() {
  const objects = [];
  let continuationToken;

  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: OUTPUT_BUCKET,
        ContinuationToken: continuationToken,
      })
    );
    if (resp.Contents) {
      objects.push(...resp.Contents);
    }
    continuationToken = resp.IsTruncated ? resp.NextContinuationToken : undefined;
  } while (continuationToken);

  return objects;
}

/**
 * Filter S3 keys to BBAU axis files within date range, selecting latest version per channel+date.
 */
function selectLatestFiles(objects, dateRange, channelFilter) {
  const fileMap = {};

  for (const obj of objects) {
    const key = obj.Key;
    const match = key.match(FILE_REGEX);
    if (!match) continue;

    const [, channel, dateStr, versionStr] = match;

    // Apply channel filter if specified
    if (channelFilter && channel.toLowerCase() !== channelFilter.toLowerCase()) continue;

    // Check channel is valid BBAU
    if (!CHANNEL_MAP[channel]) continue;

    const fileDate = parseDateFromFilename(dateStr);
    if (fileDate < dateRange.from || fileDate >= dateRange.to) continue;

    const version = parseInt(versionStr, 10);
    const groupKey = `${channel}_${dateStr}`;

    if (!fileMap[groupKey] || version > fileMap[groupKey].version) {
      fileMap[groupKey] = {
        key,
        channel,
        dateStr,
        date: formatDate(fileDate),
        version,
      };
    }
  }

  return Object.values(fileMap);
}

/**
 * Extract an attribute value from an XML tag string.
 */
function extractAttr(tag, attr) {
  const m = tag.match(new RegExp(`${attr}="([^"]*)"`));
  return m ? m[1] : "";
}

/**
 * Add HH:MM:SS duration to a HH:MM:SS start time, returning HH:MM:SS.
 */
function addTimes(start, duration) {
  const [sh, sm, ss] = start.split(":").map(Number);
  const [dh, dm, ds] = duration.split(":").map(Number);

  let totalSeconds = ss + ds;
  let totalMinutes = sm + dm + Math.floor(totalSeconds / 60);
  let totalHours = sh + dh + Math.floor(totalMinutes / 60);

  const endS = totalSeconds % 60;
  const endM = totalMinutes % 60;
  const endH = totalHours % 24; // wrap at midnight if needed

  return [
    String(endH).padStart(2, "0"),
    String(endM).padStart(2, "0"),
    String(endS).padStart(2, "0"),
  ].join(":");
}

/**
 * Parse the Date attribute on <Event> tags.
 * Format: DD-MM-YY_HH:MM:SS:FF (Sydney time)
 * Returns { date, start, duration } where times have frames stripped.
 * duration is provided separately and pre-parsed.
 */
function parseEventDate(dateAttr, durationHHMMSS) {
  // dateAttr example: "01-04-26_06:59:02:00"
  const [datePart, timePart] = dateAttr.split("_");
  // Strip frames from time: HH:MM:SS:FF → HH:MM:SS
  const start = timePart ? timePart.split(":").slice(0, 3).join(":") : "";
  const end = start && durationHHMMSS ? addTimes(start, durationHHMMSS) : "";

  return {
    date: datePart || "",
    start,
    end,
    duration: durationHHMMSS || "",
  };
}

/**
 * Strip frames from a HH:MM:SS:FF duration string, returning HH:MM:SS.
 */
function stripFrames(hhmmssff) {
  if (!hhmmssff) return "";
  return hhmmssff.split(":").slice(0, 3).join(":");
}

/**
 * Download and parse a single axis XML file for blackout events.
 * The XML uses <programme> elements with attributes (not child elements):
 *   <programme duration="..." event_id="..." start_time="..." blackout="true" ...>
 */
async function parseAxisFile(fileInfo) {
  const resp = await s3.send(
    new GetObjectCommand({ Bucket: OUTPUT_BUCKET, Key: fileInfo.key })
  );
  const xml = await resp.Body.transformToString();

  const blackoutEventIds = [];
  const programmeRegex = /<programme\s([^>]+)>/g;
  let tagMatch;
  let totalEvents = 0;

  while ((tagMatch = programmeRegex.exec(xml)) !== null) {
    const attrs = tagMatch[1];
    totalEvents++;

    if (extractAttr(attrs, "blackout") !== "true") continue;

    const eventId = extractAttr(attrs, "event_id");
    // The mapper appends "_blackout" to the original event ID
    const inputEventId = eventId.replace(/_blackout$/, "");

    blackoutEventIds.push({ eventId, inputEventId });
  }

  return { blackoutEventIds, totalEvents };
}

/**
 * Decode XML entities to plain text.
 */
function decodeXmlEntities(str) {
  return str
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code, 10)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
}

/**
 * Read an input file and build a map of EventID → full event data.
 */
async function readInputFileEvents(inputFileKey) {
  try {
    const resp = await s3.send(
      new GetObjectCommand({ Bucket: INPUT_BUCKET, Key: inputFileKey })
    );
    const xml = await resp.Body.transformToString();

    const eventMap = {};
    // Input XML has <Event Date="..."> blocks with child elements
    const eventRegex = /<Event([^>]*)>([\s\S]*?)<\/Event>/g;
    let match;

    while ((match = eventRegex.exec(xml)) !== null) {
      const attrStr = match[1];
      const block = match[2];

      const eventIdMatch = block.match(/<EventID>([^<]*)<\/EventID>/);
      if (!eventIdMatch) continue;

      const eventId = eventIdMatch[1].trim();

      const materialIdMatch = block.match(/<MaterialID>([^<]*)<\/MaterialID>/);
      const titleMatch = block.match(/<Title>([^<]*)<\/Title>/);
      const vpidMatch = block.match(/<Vpid>([^<]*)<\/Vpid>/);
      const materialTypeMatch = block.match(/<MaterialType>([^<]*)<\/MaterialType>/);
      const durationMatch = block.match(/<Duration>([^<]*)<\/Duration>/);
      const dateMatch = attrStr.match(/Date="([^"]*)"/);

      const rawDuration = durationMatch ? durationMatch[1].trim() : "";
      const durationHHMMSS = stripFrames(rawDuration);
      const dateAttr = dateMatch ? dateMatch[1] : "";
      const scheduledTime = dateAttr ? parseEventDate(dateAttr, durationHHMMSS) : null;

      eventMap[eventId] = {
        material_id: decodeXmlEntities(materialIdMatch ? materialIdMatch[1].trim() : ""),
        title: decodeXmlEntities(titleMatch ? titleMatch[1].trim() : ""),
        vpid: decodeXmlEntities(vpidMatch ? vpidMatch[1].trim() : ""),
        material_type: decodeXmlEntities(materialTypeMatch ? materialTypeMatch[1].trim() : ""),
        scheduled_time: scheduledTime,
      };
    }

    return eventMap;
  } catch (err) {
    console.warn(`Could not read input file ${inputFileKey}:`, err.message);
    return null;
  }
}

/**
 * Generate a pre-signed URL for an S3 object.
 */
async function generateSignedUrl(bucket, key) {
  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  return getSignedUrl(s3, command, { expiresIn: SIGNED_URL_EXPIRY });
}

/**
 * Format the signed URL expiry as an ISO date string.
 */
function signedUrlExpiresAt() {
  return new Date(Date.now() + SIGNED_URL_EXPIRY * 1000).toISOString();
}

/**
 * Process files in batches, collecting blackout event IDs and total event counts.
 */
async function processAxisFilesInBatches(files) {
  const fileResults = [];

  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE);
    const results = await Promise.all(batch.map(parseAxisFile));

    for (let j = 0; j < results.length; j++) {
      fileResults.push({
        fileInfo: batch[j],
        blackoutEventIds: results[j].blackoutEventIds,
        totalEvents: results[j].totalEvents,
      });
    }
  }

  return fileResults;
}

/**
 * Build the affected_files response structure by reading input files and generating signed URLs.
 */
async function buildAffectedFiles(fileResults) {
  // Only process files that have blackouts
  const filesWithBlackouts = fileResults.filter((r) => r.blackoutEventIds.length > 0);

  const affectedFiles = [];
  const expiresAt = signedUrlExpiresAt();

  // Process in batches of BATCH_SIZE (input file reads + signed URL generation)
  for (let i = 0; i < filesWithBlackouts.length; i += BATCH_SIZE) {
    const batch = filesWithBlackouts.slice(i, i + BATCH_SIZE);

    const batchResults = await Promise.all(
      batch.map(async ({ fileInfo, blackoutEventIds, totalEvents }) => {
        const inputFileKey = `export/${fileInfo.key.replace("_axis.xml", ".xml")}`;

        // Read input file events and generate signed URLs in parallel
        const [eventMap, inputSignedUrl, axisSignedUrl] = await Promise.all([
          readInputFileEvents(inputFileKey),
          generateSignedUrl(INPUT_BUCKET, inputFileKey),
          generateSignedUrl(OUTPUT_BUCKET, fileInfo.key),
        ]);

        // Build affected_events list
        const affectedEvents = blackoutEventIds.map(({ inputEventId }) => {
          if (!eventMap) {
            return {
              event_id: inputEventId,
              material_id: "",
              title: "",
              vpid: "",
              material_type: "",
              scheduled_time: null,
              reason: "Unable to read input file",
            };
          }

          const eventData = eventMap[inputEventId];

          if (!eventData) {
            return {
              event_id: inputEventId,
              material_id: "",
              title: "",
              vpid: "",
              material_type: "",
              scheduled_time: null,
              reason: "Event not found in input file",
            };
          }

          const vpid = eventData.vpid;
          const reason =
            vpid === "" ? "VPID is empty" : "VPID not found in Movida";

          return {
            event_id: inputEventId,
            material_id: eventData.material_id,
            title: eventData.title,
            vpid,
            material_type: eventData.material_type,
            scheduled_time: eventData.scheduled_time,
            reason,
          };
        });

        const blackoutCount = affectedEvents.length;
        const blackoutRate =
          totalEvents > 0
            ? (blackoutCount / totalEvents * 100).toFixed(1) + "%"
            : "0.0%";

        return {
          channel: fileInfo.channel,
          channel_code: CHANNEL_MAP[fileInfo.channel],
          schedule_date: fileInfo.date,
          file_version: fileInfo.version,
          input_file: {
            bucket: INPUT_BUCKET,
            key: inputFileKey,
            download_url: inputSignedUrl,
            download_url_expires_at: expiresAt,
          },
          axis_file: {
            key: fileInfo.key,
            download_url: axisSignedUrl,
          },
          blackout_count: blackoutCount,
          total_events: totalEvents,
          blackout_rate: blackoutRate,
          affected_events: affectedEvents,
        };
      })
    );

    affectedFiles.push(...batchResults);
  }

  // Sort by schedule_date then channel
  affectedFiles.sort((a, b) => {
    const dateCompare = a.schedule_date.localeCompare(b.schedule_date);
    return dateCompare !== 0 ? dateCompare : a.channel.localeCompare(b.channel);
  });

  return affectedFiles;
}

/**
 * Build summary statistics from file results and affected files.
 */
function buildSummary(fileResults, affectedFiles) {
  let totalBlackouts = 0;
  let totalEventsScanned = 0;
  const byChannel = {};
  const byDate = {};
  const byReason = {};

  // Aggregate channel and total event stats from all file results
  for (const { fileInfo, blackoutEventIds, totalEvents } of fileResults) {
    const ch = fileInfo.channel;
    if (!byChannel[ch]) byChannel[ch] = { blackouts: 0, total_events: 0 };
    byChannel[ch].blackouts += blackoutEventIds.length;
    byChannel[ch].total_events += totalEvents;
    totalBlackouts += blackoutEventIds.length;
    totalEventsScanned += totalEvents;
  }

  // Aggregate by_date and by_reason from affected_files
  for (const file of affectedFiles) {
    const date = file.schedule_date;
    const ch = file.channel;

    for (const event of file.affected_events) {
      if (!byDate[date]) byDate[date] = {};
      byDate[date][ch] = (byDate[date][ch] || 0) + 1;

      byReason[event.reason] = (byReason[event.reason] || 0) + 1;
    }
  }

  // Sort by_date
  const sortedByDate = {};
  for (const date of Object.keys(byDate).sort()) {
    sortedByDate[date] = byDate[date];
  }

  const blackoutRate =
    totalEventsScanned > 0
      ? (totalBlackouts / totalEventsScanned * 100).toFixed(1) + "%"
      : "0.0%";

  return {
    total_blackouts: totalBlackouts,
    total_events_scanned: totalEventsScanned,
    blackout_rate: blackoutRate,
    files_scanned: fileResults.length,
    input_files_affected: affectedFiles.length,
    by_channel: byChannel,
    by_date: sortedByDate,
    by_reason: byReason,
  };
}

export const handler = async (event) => {
  try {
    // Parse query parameters
    const params = event.queryStringParameters || {};
    const days = params.days ? parseInt(params.days, 10) : DEFAULT_BLACKOUT_DAYS;
    const channelFilter = params.channel || null;

    // Validate days
    if (isNaN(days) || days < 1 || days > 365) {
      return {
        statusCode: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: "Invalid 'days' parameter. Must be 1-365." }),
      };
    }

    // Validate channel filter
    if (channelFilter && !CHANNEL_MAP[channelFilter]) {
      return {
        statusCode: 400,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          error: `Invalid 'channel' parameter. Must be one of: ${Object.keys(CHANNEL_MAP).join(", ")}`,
        }),
      };
    }

    const dateRange = getDateRange(days);

    // List and filter axis files
    const objects = await listAllObjects();
    const files = selectLatestFiles(objects, dateRange, channelFilter);

    // Parse axis files for blackout event IDs and total event counts
    const fileResults = await processAxisFilesInBatches(files);

    // Build the input-first affected_files response (reads input files + generates signed URLs)
    const affectedFiles = await buildAffectedFiles(fileResults);

    // Build summary statistics
    const summary = buildSummary(fileResults, affectedFiles);

    // Build deduplicated affected_materials list — the actionable output for Editorial.
    // Groups all blackout occurrences by material_id so Editorial can fix each asset once.
    const materialMap = {};
    for (const file of affectedFiles) {
      for (const evt of file.affected_events) {
        const mid = evt.material_id || "UNKNOWN";
        if (!materialMap[mid]) {
          materialMap[mid] = {
            material_id: mid,
            title: evt.title,
            vpid: evt.vpid,
            reason: evt.reason,
            channels: new Set(),
            occurrences: [],
            occurrence_count: 0,
          };
        }
        materialMap[mid].channels.add(file.channel);
        materialMap[mid].occurrences.push({
          schedule_date: file.schedule_date,
          channel: file.channel,
          start: evt.scheduled_time ? evt.scheduled_time.start : "",
          end: evt.scheduled_time ? evt.scheduled_time.end : "",
        });
        materialMap[mid].occurrence_count++;
        // Keep the most informative vpid (non-empty wins)
        if (evt.vpid && !materialMap[mid].vpid) {
          materialMap[mid].vpid = evt.vpid;
        }
      }
    }
    const affectedMaterials = Object.values(materialMap)
      .map((m) => ({
        material_id: m.material_id,
        title: m.title,
        vpid: m.vpid,
        reason: m.reason,
        channels: [...m.channels].sort(),
        occurrences: m.occurrences.sort((a, b) => a.schedule_date.localeCompare(b.schedule_date) || a.start.localeCompare(b.start)),
        occurrence_count: m.occurrence_count,
      }))
      .sort((a, b) => b.occurrence_count - a.occurrence_count);

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        generated_at: new Date().toISOString(),
        date_range: {
          from: formatDate(dateRange.from),
          to: formatDate(dateRange.to),
        },
        days,
        timezone: "Australia/Sydney",
        summary,
        affected_materials: affectedMaterials,
        affected_files: affectedFiles,
      }),
    };
  } catch (err) {
    console.error("Error processing blackout request:", err);
    return {
      statusCode: 500,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        error: "Failed to process blackout request",
        message: err.message,
      }),
    };
  }
};
