/* eslint-disable no-unused-vars, */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import createSruClient from '@natlibfi/sru-client';
import {MARCXML} from '@natlibfi/marc-record-serializers';

export function sruOperator(sruUrl) {
  const logger = createLogger();
  const sruClient = createSruClient({url: sruUrl, recordSchema: 'marcxml', maxRecordsPerRequest: 500, retrieveAll: false});
  // Max records per request seems to be 50

  return {getRecords};

  async function getRecords(query, offset = 0) {
    logger.log('info', 'Executing queries');

    try {
      // Execute queries
      const results = await getRecord(query, offset);
      logger.log('debug', 'Got link data');
      return results;
    } catch (error) {
      logger.log('debug', 'Error while searching link data');
      logger.log('error', error);
      return false;
    }

    function getRecord(query, offset) {
      return new Promise((resolve, reject) => {
        const promises = [];
        sruClient.searchRetrieve(query, {startRecord: offset})
          .on('record', xmlString => {
            logger.log('silly', 'Got Record');
            promises.push(MARCXML.from(xmlString, {subfieldValues: false})); // eslint-disable-line functional/immutable-data
          })
          .on('end', async offset => {
            logger.log('info', 'Ending queries');
            const records = await Promise.all(promises);
            resolve({offset, records});
          })
          .on('error', err => reject(err));
      });
    }
  }
}
