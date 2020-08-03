/* eslint-disable no-unused-vars */
import {promisify} from 'util';
import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {eratuontiFactory, COMMON_JOB_STATES, IMPORTER_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons';

export async function importToErätuonti(jobId, jobConfig, mongoOperator, amqpOperator, {apiUrl, apiUsername, apiPassword, apiClientUserAgent}) {
  const {createLogger} = Utils;
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const {linkDataHarvesterApiProfileId} = jobConfig;
  const eratuontiOperator = eratuontiFactory({apiUrl, apiUsername, apiPassword, apiClientUserAgent, linkDataHarvesterApiProfileId}); // eslint-disable-line no-unused-vars

  await mongoOperator.setState({jobId, state: IMPORTER_JOB_STATES.PROCESSING_ERATUONTI_IMPORT});

  await pumpMessagesFromQueue();
  // Await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});

  return true;

  async function pumpMessagesFromQueue() {
    const amqpMessages = await amqpOperator.checkQueue(`${IMPORTER_JOB_STATES.PENDING_ERATUONTI_IMPORT}.${jobId}`);
    if (!amqpMessages) {
      return;
    }

    const linkedData = amqpMessages.map(message => JSON.parse(message.content.toString()));

    if (linkedData.length > 0) { // eslint-disable-line functional/no-conditional-statement
      // Send each or all? pump?
      const blobId = eratuontiOperator.sendBlob(linkedData);

      // Link data has been sent to erätuonti
      logger.log('debug', blobId);
      await mongoOperator.pushBlobIds({jobId, blobIds: [blobId]});
      await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
      await amqpOperator.ackMessages(amqpMessages);

      return pumpMessagesFromQueue();
    }

    // Await mongoOperator.setState({jobId, state: JOB_STATES.DONE});
    await amqpOperator.ackMessages(amqpMessages);
    return pumpMessagesFromQueue();
  }
}
