import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_REGION || "eu-west-1" });
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || "britbox-epg-schedule-p7-output-stage";
const INPUT_BUCKET = process.env.INPUT_BUCKET || "britbox-epg-schedule-p7-stage";
const DEFAULT_BLACKOUT_DAYS = parseInt(process.env.BLACKOUT_DAYS || "30", 10);

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
 * Download and parse a single axis XML file for blackout events.
 * The XML uses <programme> elements with attributes (not child elements):
 *   <programme duration="..." event_id="..." start_time="..." blackout="true" ...>
 */
async function parseAxisFile(fileInfo) {
  const resp = await s3.send(
    new GetObjectCommand({ Bucket: OUTPUT_BUCKET, Key: fileInfo.key })
  );
  const xml = await resp.Body.transformToString();

  const blackouts = [];
  // Match each <programme ...> opening tag (self-closing or with children)
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
    // Input file is the same name without "_axis", under export/ prefix
    const inputFileKey = `export/${fileInfo.key.replace("_axis.xml", ".xml")}`;

    blackouts.push({
      channel: fileInfo.channel,
      channel_code: CHANNEL_MAP[fileInfo.channel],
      date: fileInfo.date,
      event_id: eventId,
      input_event_id: inputEventId,
      start_time: extractAttr(attrs, "start_time"),
      end_time: extractAttr(attrs, "end_time"),
      duration: extractAttr(attrs, "duration"),
      source_file: fileInfo.key,
      input_file: inputFileKey,
      file_version: fileInfo.version,
    });
  }

  return { blackouts, totalEvents };
}

/**
 * Read an input file and build a map of EventID → Vpid.
 */
async function readInputFileVpids(inputFileKey) {
  try {
    const resp = await s3.send(
      new GetObjectCommand({ Bucket: INPUT_BUCKET, Key: inputFileKey })
    );
    const xml = await resp.Body.transformToString();

    const vpidMap = {};
    // Input XML has <Event> blocks with <EventID> and <Vpid> child elements
    const eventRegex = /<Event[^>]*>([\s\S]*?)<\/Event>/g;
    let match;

    while ((match = eventRegex.exec(xml)) !== null) {
      const block = match[1];
      const eventIdMatch = block.match(/<EventID>([^<]*)<\/EventID>/);
      const vpidMatch = block.match(/<Vpid>([^<]*)<\/Vpid>/);
      if (eventIdMatch) {
        vpidMap[eventIdMatch[1]] = vpidMatch ? vpidMatch[1].trim() : "";
      }
    }

    return vpidMap;
  } catch (err) {
    console.warn(`Could not read input file ${inputFileKey}:`, err.message);
    return null;
  }
}

/**
 * Enrich blackouts with reason by reading corresponding input files.
 */
async function enrichBlackoutReasons(allBlackouts) {
  // Group blackouts by input file
  const byInputFile = {};
  for (const b of allBlackouts) {
    if (!byInputFile[b.input_file]) byInputFile[b.input_file] = [];
    byInputFile[b.input_file].push(b);
  }

  // Read input files in batches
  const inputFileKeys = Object.keys(byInputFile);
  for (let i = 0; i < inputFileKeys.length; i += BATCH_SIZE) {
    const batch = inputFileKeys.slice(i, i + BATCH_SIZE);
    const vpidMaps = await Promise.all(batch.map(readInputFileVpids));

    for (let j = 0; j < batch.length; j++) {
      const vpidMap = vpidMaps[j];
      const blackouts = byInputFile[batch[j]];

      for (const b of blackouts) {
        if (!vpidMap) {
          b.reason = "Unable to read input file";
          b.vpid = "";
          continue;
        }
        const vpid = vpidMap[b.input_event_id];
        if (vpid === undefined) {
          b.reason = "Event not found in input file";
          b.vpid = "";
        } else if (vpid === "") {
          b.reason = "VPID is empty";
          b.vpid = "";
        } else {
          b.reason = "VPID not found in Movida";
          b.vpid = vpid;
        }
      }
    }
  }
}

/**
 * Process files in batches to avoid overwhelming S3.
 */
async function processFilesInBatches(files) {
  const allBlackouts = [];
  const channelStats = {};

  for (let i = 0; i < files.length; i += BATCH_SIZE) {
    const batch = files.slice(i, i + BATCH_SIZE);
    const results = await Promise.all(batch.map(parseAxisFile));

    for (let j = 0; j < results.length; j++) {
      const { blackouts, totalEvents } = results[j];
      const channel = batch[j].channel;

      if (!channelStats[channel]) {
        channelStats[channel] = { blackouts: 0, total_events: 0 };
      }
      channelStats[channel].blackouts += blackouts.length;
      channelStats[channel].total_events += totalEvents;

      allBlackouts.push(...blackouts);
    }
  }

  // Enrich with blackout reasons from input files
  await enrichBlackoutReasons(allBlackouts);

  return { allBlackouts, channelStats };
}

/**
 * Build the by_date summary from blackout events.
 */
function buildByDateSummary(blackouts) {
  const byDate = {};

  for (const b of blackouts) {
    if (!byDate[b.date]) {
      byDate[b.date] = {};
    }
    byDate[b.date][b.channel] = (byDate[b.date][b.channel] || 0) + 1;
  }

  // Sort by date
  const sorted = {};
  for (const date of Object.keys(byDate).sort()) {
    sorted[date] = byDate[date];
  }
  return sorted;
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

    // List and filter files
    const objects = await listAllObjects();
    const files = selectLatestFiles(objects, dateRange, channelFilter);

    // Process XML files
    const { allBlackouts, channelStats } = await processFilesInBatches(files);

    // Sort blackouts by date then start_time
    allBlackouts.sort((a, b) => a.start_time.localeCompare(b.start_time));

    const totalBlackouts = allBlackouts.length;
    const byDate = buildByDateSummary(allBlackouts);

    // Build unique list of affected input files with their blackout event IDs
    const inputFileMap = {};
    for (const b of allBlackouts) {
      if (!inputFileMap[b.input_file]) {
        inputFileMap[b.input_file] = {
          bucket: INPUT_BUCKET,
          key: b.input_file,
          channel: b.channel,
          date: b.date,
          version: b.file_version,
          event_ids: [],
        };
      }
      inputFileMap[b.input_file].event_ids.push(b.input_event_id);
    }
    const inputFilesAffected = Object.values(inputFileMap);

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
        input_bucket: INPUT_BUCKET,
        summary: {
          total_blackouts: totalBlackouts,
          files_processed: files.length,
          input_files_affected: inputFilesAffected.length,
          by_channel: channelStats,
          by_date: byDate,
        },
        input_files_affected: inputFilesAffected,
        blackouts: allBlackouts,
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
