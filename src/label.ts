import { ComAtprotoLabelDefs } from '@atcute/client/lexicons';
import { LabelerServer } from '@skyware/labeler';
import { DID, SIGNING_KEY } from './config.js';
import { DELETE, LABELS, LABEL_LIMIT } from './constants.js';
import logger from './logger.js';

export const labelerServer = new LabelerServer({ did: DID, signingKey: SIGNING_KEY });

export const label = (did: string, rkey: string) => {
  logger.info('\n=== Processing Label Request ===');
  logger.info(`User: ${did}`);
  logger.info(`Post: ${rkey}`);

  if (rkey === 'self') {
    logger.info('Self-like detected - ignoring');
    return;
  }

  try {
    const labels = fetchCurrentLabels(did);
    
    if (rkey.includes(DELETE)) {
      logger.info('=== Delete Request ===');
      deleteAllLabels(did, labels);
    } else {
      logger.info('=== Label Request ===');
      addOrUpdateLabel(did, rkey, labels);
    }
  } catch (error) {
    logger.error('Label processing error:', error);
  }
};

function fetchCurrentLabels(did: string) {
  logger.info('Checking current labels...');
  const query = labelerServer.db
    .prepare<string[]>(`SELECT * FROM labels WHERE uri = ?`)
    .all(did) as ComAtprotoLabelDefs.Label[];

  const labels = query.reduce((set, label) => {
    if (!label.neg) set.add(label.val);
    else set.delete(label.val);
    return set;
  }, new Set<string>());

  if (labels.size > 0) {
    logger.info(`Found labels: ${Array.from(labels).join(', ')}`);
  } else {
    logger.info('No existing labels found');
  }
  
  return labels;
}

function deleteAllLabels(did: string, labels: Set<string>) {
  const labelsToDelete: string[] = Array.from(labels);

  if (labelsToDelete.length === 0) {
    logger.info('No labels to delete');
    return;
  }

  logger.info(`Deleting labels: ${labelsToDelete.join(', ')}`);
  try {
    labelerServer.createLabels({ uri: did }, { negate: labelsToDelete });
    logger.info('✓ Labels deleted successfully');
  } catch (error) {
    logger.error('Failed to delete labels:', error);
  }
}

function addOrUpdateLabel(did: string, rkey: string, labels: Set<string>) {
  const newLabel = LABELS.find((label) => label.rkey === rkey);
  if (!newLabel) {
    logger.warn('Invalid label request - post not found in label list');
    return;
  }

  logger.info(`Requested label: ${newLabel.identifier}`);

  if (labels.size >= LABEL_LIMIT) {
    logger.info(`Label limit (${LABEL_LIMIT}) reached - removing old labels`);
    try {
      labelerServer.createLabels({ uri: did }, { negate: Array.from(labels) });
      logger.info('✓ Old labels removed');
    } catch (error) {
      logger.error('Failed to remove old labels:', error);
    }
  }

  try {
    labelerServer.createLabel({ uri: did, val: newLabel.identifier });
    logger.info(`✓ Label "${newLabel.identifier}" applied successfully`);
  } catch (error) {
    logger.error('Failed to apply new label:', error);
  }
}
