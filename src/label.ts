import { ComAtprotoLabelDefs } from '@atcute/client/lexicons';
import { LabelerServer } from '@skyware/labeler';
import { DID, SIGNING_KEY } from './config.js';
import { DELETE, LABELS, LABEL_LIMIT } from './constants.js';
import logger from './logger.js';

export const labelerServer = new LabelerServer({ did: DID, signingKey: SIGNING_KEY });

export const label = (did: string, rkey: string) => {
  if (rkey === 'self') return;

  try {
    const labels = fetchCurrentLabels(did);
    
    if (rkey.includes(DELETE)) {
      deleteAllLabels(did, labels);
    } else {
      addOrUpdateLabel(did, rkey, labels);
    }
  } catch (error) {
    logger.error('Label error:', error);
  }
};

function fetchCurrentLabels(did: string) {
  const query = labelerServer.db
    .prepare<string[]>(`SELECT * FROM labels WHERE uri = ?`)
    .all(did) as ComAtprotoLabelDefs.Label[];

  return query.reduce((set, label) => {
    if (!label.neg) set.add(label.val);
    else set.delete(label.val);
    return set;
  }, new Set<string>());
}

function deleteAllLabels(did: string, labels: Set<string>) {
  const labelsToDelete: string[] = Array.from(labels);
  if (labelsToDelete.length === 0) return;

  try {
    labelerServer.createLabels({ uri: did }, { negate: labelsToDelete });
    logger.info('Labels deleted');
  } catch (error) {
    logger.error('Delete error:', error);
  }
}

function addOrUpdateLabel(did: string, rkey: string, labels: Set<string>) {
  const newLabel = LABELS.find((label) => label.rkey === rkey);
  if (!newLabel) return;

  if (labels.size >= LABEL_LIMIT) {
    try {
      labelerServer.createLabels({ uri: did }, { negate: Array.from(labels) });
    } catch (error) {
      logger.error('Error removing old labels:', error);
    }
  }

  try {
    labelerServer.createLabel({ uri: did, val: newLabel.identifier });
    logger.info(`Label applied: ${newLabel.identifier}`);
  } catch (error) {
    logger.error('Error adding label:', error);
  }
}
