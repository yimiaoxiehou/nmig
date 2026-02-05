/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
 */
import { parentPort, workerData } from 'node:worker_threads';

import Conversion from './conversion';
import { createTable } from './table-processor';
import prepareDataChunks from './data-chunks-processor';
import { readDataAndIndexTypesMap } from './fs-ops';

type WorkerInput = {
  config: any;
  tableName: string;
  haveDataChunksProcessed: boolean;
  skipPgTableCreation: boolean;
  mysqlVersion: string;
};

const run = async (): Promise<void> => {
  const data: WorkerInput = workerData as WorkerInput;

  // Avoid creating nested logger process in worker thread context.
  const avoidLogger = true;
  const conversion = new Conversion(data.config, avoidLogger);
  conversion._mysqlVersion = data.mysqlVersion;

  conversion._dicTables.set(data.tableName, {
    tableLogPath: `${conversion._logsDirPath}/${data.tableName}.log`,
    arrTableColumns: [],
  });

  await readDataAndIndexTypesMap(conversion);
  await createTable(conversion, data.tableName, data.skipPgTableCreation);
  await prepareDataChunks(conversion, data.tableName, data.haveDataChunksProcessed);
};

void run()
  .then(() => parentPort?.postMessage({ ok: true }))
  .catch(error => {
    parentPort?.postMessage({
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  });
