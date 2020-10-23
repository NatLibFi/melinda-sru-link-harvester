/* eslint-disable no-unused-vars, no-undef, no-warning-comments */
import {promisify} from 'util';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {collect} from './interfaces/collect';
import {mongoFactory, HARVESTER_JOB_STATES, VALIDATOR_JOB_STATES, IMPORTER_JOB_STATES, amqpFactory} from '@natlibfi/melinda-record-link-migration-commons';

export default async function ({
  apiUrl, apiUsername, apiPassword, apiClientUserAgent, mongoUrl, amqpUrl
}) {
  const logger = createLogger();
  const eratuontiConfig = {apiUrl, apiUsername, apiPassword, apiClientUserAgent};
  const mongoOperator = await mongoFactory(mongoUrl);
  const amqpOperator = await amqpFactory(amqpUrl);
  const setTimeoutPromise = promisify(setTimeout);
  logger.log('info', 'Melinda-sru-harvester has started');

  return check();

  async function check(wait) {
    if (wait) {
      await setTimeoutPromise(3000);
      return check();
    }

    // If job state PROCESSING_SRU_HARVESTING => collect or , VALIDATING => validate or  => import continue it!

    // Collect
    await checkJobsInState(HARVESTER_JOB_STATES.PROCESSING_SRU_HARVESTING);
    await checkJobsInState(HARVESTER_JOB_STATES.PENDING_SRU_HARVESTER);

    // Loop
    return check(true);
  }

  async function checkJobsInState(state) {
    const job = await mongoOperator.getOne(state);
    // Logger.log('debug', JSON.stringify(job, undefined, ' '));
    if (job === undefined || job === null) { // eslint-disable-line functional/no-conditional-statement
      logger.log('info', `No job in state: ${state}`);
      return;
    }

    /* Empty queue loop for testing
    const {jobId} = job;
    await mongoOperator.setState({jobId, state: COMMON_JOB_STATES.DONE});
    */

    // Real loop
    const {jobId, jobConfig} = job;

    // Collect potential link data
    if (state === HARVESTER_JOB_STATES.PENDING_SRU_HARVESTER || state === HARVESTER_JOB_STATES.PROCESSING_SRU_HARVESTING) {
      await collect(jobId, jobConfig, mongoOperator, amqpOperator);
      return check();
    }

    return check();
  }
}
