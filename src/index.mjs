import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: process.env.AWS_REGION || "eu-west-1" });
const OUTPUT_BUCKET = process.env.OUTPUT_BUCKET || "britbox-epg-schedule-p7-output-stage";
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

    blackouts.push({
      channel: fileInfo.channel,
      channel_code: CHANNEL_MAP[fileInfo.channel],
      date: fileInfo.date,
      event_id: extractAttr(attrs, "event_id"),
      start_time: extractAttr(attrs, "start_time"),
      end_time: extractAttr(attrs, "end_time"),
      duration: extractAttr(attrs, "duration"),
      source_file: fileInfo.key,
      file_version: fileInfo.version,
    });
  }

  return { blackouts, totalEvents };
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
        summary: {
          total_blackouts: totalBlackouts,
          files_processed: files.length,
          by_channel: channelStats,
          by_date: byDate,
        },
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
