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
} catch (error) {
  if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
    cursor = Math.floor(Date.now() * 1000);
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
  logger.info('Jetstream connected');
  cursorUpdateInterval = setInterval(() => {
    if (jetstream.cursor) {
      fs.writeFile('cursor.txt', jetstream.cursor.toString(), (err) => {
        if (err) logger.error(err);
      });
    }
  }, CURSOR_UPDATE_INTERVAL);
});

jetstream.onCreate(WANTED_COLLECTION, (event: CommitCreateEvent<typeof WANTED_COLLECTION>) => {
  if (event.commit?.record?.subject?.uri?.includes(DID)) {
    logger.info('Label request from:', event.did);
    label(event.did, event.commit.record.subject.uri.split('/').pop()!);
  }
});

const metricsServer = startMetricsServer(METRICS_PORT);

labelerServer.app.listen({ port: PORT, host: HOST }, (error, address) => {
  if (error) {
    logger.error('Server error:', error);
  } else {
    logger.info('Labeler server started');
  }
});

jetstream.start();

process.on('SIGINT', () => {
  logger.info('Shutting down...');
  fs.writeFileSync('cursor.txt', jetstream.cursor!.toString(), 'utf8');
  jetstream.close();
  labelerServer.stop();
  metricsServer.close();
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('Shutting down...');
  fs.writeFileSync('cursor.txt', jetstream.cursor!.toString(), 'utf8');
  jetstream.close();
  labelerServer.stop();
  metricsServer.close();
  process.exit(0);
});
