/* eslint-disable no-unused-vars, */
import {Utils} from '@natlibfi/melinda-commons';
import createSruClient from '@natlibfi/sru-client';
import {MARCXML} from '@natlibfi/marc-record-serializers';

export async function getLinkedInfo(sruUrl, query, offset = 0) {
  const {createLogger, logError} = Utils;
  const logger = createLogger();
  const sruClient = createSruClient({url: sruUrl, recordSchema: 'marcxml', maxRecordsPerRequest: 500, retrieveAll: false});
  // Max records per request seems to be 50

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
      const records = [];
      sruClient.searchRetrieve(query, {startRecord: offset})
        .on('record', xmlString => {
          logger.log('silly', 'Got Record');
          records.push(MARCXML.from(xmlString)); // eslint-disable-line functional/immutable-data
        })
        .on('end', offset => {
          logger.log('info', 'Ending queries');
          resolve({offset, records});
        })
        .on('error', err => reject(err));
    });
  }
}
