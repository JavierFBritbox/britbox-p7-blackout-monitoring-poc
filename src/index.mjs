export const handler = async (event) => {
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      message: "P7 Blackout Monitoring API - placeholder",
      environment: process.env.OUTPUT_BUCKET || "unknown",
      blackoutDays: process.env.BLACKOUT_DAYS || "30",
      timestamp: new Date().toISOString(),
    }),
  };
};
