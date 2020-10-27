/* eslint-disable no-unused-vars */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {getFromRecord} from '../util';
import {sruOperator} from './sru';
import {MarcRecord} from '@natlibfi/marc-record';
import {format, promisify} from 'util';
import {COMMON_JOB_STATES, HARVESTER_JOB_STATES, VALIDATOR_JOB_STATES} from '@natlibfi/melinda-record-link-migration-commons/dist/constants';

export async function collect(jobId, jobConfig, mongoOperator, amqpOperator) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const {sourceRecord, linkDataHarvestSearch} = jobConfig;
  const sruClient = await sruOperator(linkDataHarvestSearch.url);
  const marcRecord = new MarcRecord(sourceRecord);
  await mongoOperator.setState({jobId, state: HARVESTER_JOB_STATES.PROCESSING_SRU_HARVESTING});

  logger.log('info', 'Generating queries');
  const [queryValue] = getFromRecord(linkDataHarvestSearch.from, marcRecord);
  logger.log('debug', queryValue);
  const normalizedQueryValue = normalizeQuerys(queryValue);
  const query = format(linkDataHarvestSearch.queryFormat, normalizedQueryValue);
  logger.log('debug', query);

  logger.log('debug', 'Get link data');
  logger.log('debug', `Start offset: ${linkDataHarvestSearch.offset}`);
  await pump(query, linkDataHarvestSearch.offset);

  await setTimeoutPromise(50); // Makes sure last record gets in amqp queue

  const messages = await amqpOperator.checkQueue(`${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`, 'messages');
  if (messages === 0 || !messages) {
    logger.log('debug', JSON.stringify(messages));
    await amqpOperator.removeQueue(`${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`);
    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
    return false;
  }

  logger.log('debug', `Got all records. ${messages} records sent for VALIDATION!`);
  await mongoOperator.setState({jobId, state: VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING});
  return true;

  async function pump(query, count = 1) {
    const result = await sruClient.getRecords(query, count);

    if (result === false) { // In case of connection error
      await setTimeoutPromise(5000);
      return pump(query, count);
    }

    const {offset, records} = result;
    logger.log('verbose', `Handling records ${count} - ${offset - 1}`);

    if (isNaN(offset) && records === undefined) {
      return;
    }

    await pumpToAmqp(records, jobId);

    const updateOffset = offset || count + records.length;
    const updatedJobConfig = createUpdatedJobConfig(jobConfig, updateOffset);
    await mongoOperator.updateJobConfig({jobId, jobConfig: updatedJobConfig});

    if (isNaN(offset)) {
      return;
    }

    return pump(query, offset);
  }

  async function pumpToAmqp(records, jobId) {
    const [record, ...rest] = records;
    if (record === undefined) {
      logger.log('debug', 'Records pumped to amqp queue');
      return;
    }

    const queue = `${VALIDATOR_JOB_STATES.PENDING_VALIDATION_FILTERING}.${jobId}`;
    logger.log('silly', `Sending data to: ${queue}`);
    await amqpOperator.sendToQueue({queue, correlationId: jobId, data: record});

    return pumpToAmqp(rest, jobId);
  }

  function createUpdatedJobConfig(jobConfig, offset) {
    logger.log('debug', `Offset: ${offset}`);
    return {
      sourceRecord: jobConfig.sourceRecord,
      linkDataHarvestSearch: {
        type: jobConfig.linkDataHarvestSearch.type,
        from: jobConfig.linkDataHarvestSearch.from,
        queryFormat: jobConfig.linkDataHarvestSearch.queryFormat,
        url: jobConfig.linkDataHarvestSearch.url,
        offset: offset || 0
      },
      linkDataHarvesterApiProfileId: jobConfig.linkDataHarvesterApiProfileId,
      linkDataHarvesterValidationFilters: jobConfig.linkDataHarvesterValidationFilters
    };
  }

  function normalizeQuerys(queryValue) {
    return queryValue
      .replace(/[^\w\s\p{Alphabetic}]/gu, '')
      .trim()
      .slice(0, 30)
      .trim();
  }
}
