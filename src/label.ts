import { ComAtprotoLabelDefs } from '@atcute/client/lexicons';
import { LabelerServer } from '@skyware/labeler';
import { DID, SIGNING_KEY } from './config.js';
import { DELETE, LABELS, LABEL_LIMIT } from './constants.js';
import logger from './logger.js';

export const labelerServer = new LabelerServer({ did: DID, signingKey: SIGNING_KEY });

export const label = (did: string, rkey: string) => {
  logger.info('Debug: Label function entry point');
  logger.info(`Debug: Received rkey: ${rkey} for ${did}`);
  logger.info('Debug: Current DID:', DID);

  if (rkey === 'self') {
    logger.info(`${did} liked the labeler. Returning.`);
    return;
  }

  try {
    logger.info('Debug: Fetching current labels');
    const labels = fetchCurrentLabels(did);
    logger.info('Debug: Current labels:', Array.from(labels));

    if (rkey.includes(DELETE)) {
      logger.info('Debug: Delete rkey detected');
      deleteAllLabels(did, labels);
    } else {
      logger.info('Debug: Adding/updating label');
      addOrUpdateLabel(did, rkey, labels);
    }
  } catch (error) {
    logger.error(`Error in \`label\` function: ${error}`);
  }
};

function fetchCurrentLabels(did: string) {
  logger.info('Debug: Running fetchCurrentLabels for:', did);
  const query = labelerServer.db
    .prepare<string[]>(`SELECT * FROM labels WHERE uri = ?`)
    .all(did) as ComAtprotoLabelDefs.Label[];
  
  logger.info('Debug: Raw query result:', query);

  const labels = query.reduce((set, label) => {
    if (!label.neg) set.add(label.val);
    else set.delete(label.val);
    return set;
  }, new Set<string>());

  if (labels.size > 0) {
    logger.info(`Current labels: ${Array.from(labels).join(', ')}`);
  }
  return labels;
}

function deleteAllLabels(did: string, labels: Set<string>) {
  const labelsToDelete: string[] = Array.from(labels);
  logger.info('Debug: In deleteAllLabels function');
  logger.info('Debug: Labels to delete:', labelsToDelete);

  if (labelsToDelete.length === 0) {
    logger.info(`No labels to delete`);
  } else {
    logger.info(`Labels to delete: ${labelsToDelete.join(', ')}`);
    try {
      labelerServer.createLabels({ uri: did }, { negate: labelsToDelete });
      logger.info('Successfully deleted all labels');
    } catch (error) {
      logger.error(`Error deleting all labels: ${error}`);
      logger.error('Debug: Delete error details:', error);
    }
  }
}

function addOrUpdateLabel(did: string, rkey: string, labels: Set<string>) {
  logger.info('Debug: In addOrUpdateLabel function');
  logger.info('Debug: Looking for rkey:', rkey);
  
  const newLabel = LABELS.find((label) => label.rkey === rkey);
  if (!newLabel) {
    logger.warn(`New label not found: ${rkey}. Likely liked a post that's not one for labels.`);
    logger.info('Debug: Available rkeys:', LABELS.map(l => l.rkey));
    return;
  }

  logger.info(`New label: ${newLabel.identifier}`);
  logger.info('Debug: Label limit:', LABEL_LIMIT);
  logger.info('Debug: Current labels size:', labels.size);

  if (labels.size >= LABEL_LIMIT) {
    logger.info('Debug: Attempting to negate existing labels');
    try {
      labelerServer.createLabels({ uri: did }, { negate: Array.from(labels) });
      logger.info(`Successfully negated existing labels: ${Array.from(labels).join(', ')}`);
    } catch (error) {
      logger.error(`Error negating existing labels: ${error}`);
      logger.error('Debug: Negation error details:', error);
    }
  }

  try {
    logger.info('Debug: Attempting to create new label:', newLabel.identifier);
    labelerServer.createLabel({ uri: did, val: newLabel.identifier });
    logger.info(`Successfully labeled ${did} with ${newLabel.identifier}`);
  } catch (error) {
    logger.error(`Error adding new label: ${error}`);
    logger.error('Debug: Creation error details:', error);
  }
}
