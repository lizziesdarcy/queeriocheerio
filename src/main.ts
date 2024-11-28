import { CommitCreateEvent, Jetstream } from '@skyware/jetstream';
import fs from 'node:fs';
import { CURSOR_UPDATE_INTERVAL, DID, FIREHOSE_URL, HOST, METRICS_PORT, PORT, WANTED_COLLECTION } from './config.js';
import { label, labelerServer } from './label.js';
import logger from './logger.js';
import { startMetricsServer } from './metrics.js';

let cursor = 0;
let cursorUpdateInterval: NodeJS.Timeout;

function epochUsToDateTime(cursor: number): string {
  return new Date(cursor / 1000).toISOString();
}

try {
  logger.info('Starting labeler service...');
  cursor = Number(fs.readFileSync('cursor.txt', 'utf8'));
  logger.info(`Starting from cursor: ${cursor}`);
} catch (error) {
  if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
    cursor = Math.floor(Date.now() * 1000);
    logger.info(`Creating new cursor: ${cursor}`);
    fs.writeFileSync('cursor.txt', cursor.toString(), 'utf8');
  } else {
    logger.error(error);
    process.exit(1);
  }
}

const jetstream = new Jetstream({
  wantedCollections: [WANTED_COLLECTION],
  endpoint: FIREHOSE_URL,
  cursor: cursor,
});

jetstream.on('open', () => {
  logger.info('=== Jetstream Connected ===');
  logger.info(`Endpoint: ${FIREHOSE_URL}`);
  logger.info(`Starting cursor: ${jetstream.cursor}`);
  
  cursorUpdateInterval = setInterval(() => {
    if (jetstream.cursor) {
      logger.info(`Cursor: ${jetstream.cursor}`);
      fs.writeFile('cursor.txt', jetstream.cursor.toString(), (err) => {
        if (err) logger.error(err);
      });
    }
  }, CURSOR_UPDATE_INTERVAL);
});

jetstream.on('close', () => {
  clearInterval(cursorUpdateInterval);
  logger.info('=== Jetstream Disconnected ===');
});

jetstream.on('error', (error) => {
  logger.error('=== Jetstream Error ===');
  logger.error(error.message);
});

jetstream.onCreate(WANTED_COLLECTION, (event: CommitCreateEvent<typeof WANTED_COLLECTION>) => {
  logger.info('\n=== New Like Event ===');
  logger.info('User:', event.did);
  logger.info('Post:', event.commit?.record?.subject?.uri);

  if (event.commit?.record?.subject?.uri?.includes(DID)) {
    logger.info('✓ Processing label request');
    label(event.did, event.commit.record.subject.uri.split('/').pop()!);
  } else {
    logger.info('✗ Not a label request - ignoring');
  }
  logger.info('=== End Event ===\n');
});

const metricsServer = startMetricsServer(METRICS_PORT);

labelerServer.app.listen({ port: PORT, host: HOST }, (error, address) => {
  if (error) {
    logger.error('Failed to start labeler server:', error);
  } else {
    logger.info(`Labeler server running on ${address}`);
  }
});

jetstream.start();

function shutdown() {
  try {
    logger.info('=== Shutting Down ===');
    fs.writeFileSync('cursor.txt', jetstream.cursor!.toString(), 'utf8');
    jetstream.close();
    labelerServer.stop();
    metricsServer.close();
  } catch (error) {
    logger.error('Shutdown error:', error);
    process.exit(1);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
