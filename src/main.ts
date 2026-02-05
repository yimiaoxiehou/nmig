/*
 * This file is a part of "NMIG" - the database migration tool.
 *
 * Copyright (C) 2016 - present, Anatoly Khaytovich <anatolyuss@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program (please see the "LICENSE.md" file).
 * If not, see <http://www.gnu.org/licenses/gpl.txt>.
 *
 * @author Anatoly Khaytovich <anatolyuss@gmail.com>
 */
import * as os from 'node:os';
import * as path from 'node:path';
import { Worker } from 'node:worker_threads';

import Conversion from './conversion';
import createSchema from './schema-processor';
import loadStructureToMigrate from './structure-loader';
import DataPipeManager from './data-pipe-manager';
import decodeBinaryData from './binary-data-decoder';
import generateReport from './report-generator';
import DbAccess from './db-access';
import { dropDataPoolTable } from './data-pool-manager';
import { processConstraints } from './constraints-processor';
import { getConfAndLogsPaths, boot } from './boot-processor';
import { createStateLogsTable, dropStateLogsTable } from './migration-state-manager';
import { createDataPoolTable, readDataPool, isDataPoolTableNotEmpty } from './data-pool-manager';
import * as migrationStateManager from './migration-state-manager';
import { log } from './fs-ops';
import {
  readConfig,
  readExtraConfig,
  createLogsDirectory,
  readDataAndIndexTypesMap,
} from './fs-ops';

const { confPath, logsPath } = getConfAndLogsPaths();

/**
 * Loads configuration and prepares Conversion instance.
 */
const initializeConversion = async (): Promise<Conversion> => {
  const config = await readConfig(confPath, logsPath);
  const configWithExtras = await readExtraConfig(config, confPath);
  const conversion = await Conversion.initializeConversion(configWithExtras);
  await createLogsDirectory(conversion);
  await readDataAndIndexTypesMap(conversion);
  return conversion;
};

/**
 * Resolves table-subtasks parallelism level.
 */
const getTableSubtasksConcurrency = (conversion: Conversion): number => {
  const configuredValue = conversion._numberOfSimultaneouslyRunningReaderProcesses;
  const defaultConcurrency = 2;
  const maxConfiguredConcurrency =
    configuredValue === 'DEFAULT' ? defaultConcurrency : (configuredValue as number);

  return Math.max(
    1,
    Math.min(
      os.cpus().length || 1,
      conversion._maxEachDbConnectionPoolSize,
      maxConfiguredConcurrency,
      conversion._tablesToMigrate.length,
    ),
  );
};

/**
 * Runs per-table subtasks in worker_threads pool.
 */
const runTableSubtasksInWorkerThreads = async (
  conversion: Conversion,
  haveDataChunksProcessed: boolean,
  skipPgTableCreation: boolean,
): Promise<void> => {
  if (conversion._tablesToMigrate.length === 0) {
    return;
  }

  const workersCnt = getTableSubtasksConcurrency(conversion);
  await log(
    conversion,
    `	--[Main] Running per-table subtasks in worker threads. concurrency: ${workersCnt}`,
  );

  const queue: string[] = [...conversion._tablesToMigrate];
  const workerPath = path.join(__dirname, 'table-subtask-worker.js');

  const runWorkerForTable = (tableName: string): Promise<void> => {
    return new Promise<void>((resolve, reject) => {
      const worker = new Worker(workerPath, {
        workerData: {
          config: conversion._config,
          tableName,
          haveDataChunksProcessed,
          skipPgTableCreation,
          mysqlVersion: conversion._mysqlVersion,
        },
      });

      worker.once('message', (message: any): void => {
        if (message?.ok) {
          return resolve();
        }

        return reject(new Error(message?.error || `Worker failed for table ${tableName}`));
      });

      worker.once('error', reject);
      worker.once('exit', (code: number): void => {
        if (code !== 0) {
          reject(new Error(`Worker exited with code ${code} for table ${tableName}`));
        }
      });
    });
  };

  const thread = async (): Promise<void> => {
    while (queue.length !== 0) {
      const tableName: string | undefined = queue.shift();

      if (!tableName) {
        return;
      }

      await runWorkerForTable(tableName);
    }
  };

  const threads: Promise<void>[] = [];
  for (let i = 0; i < workersCnt; ++i) {
    threads.push(thread());
  }

  await Promise.all(threads);
};

/**
 * Runs NMIG migration pipeline in a sequential and explicit way.
 */
const runMigration = async (): Promise<void> => {
  try {
    let conversion: Conversion = await initializeConversion();

    conversion = await boot(conversion);
    conversion = await createSchema(conversion);
    conversion = await createStateLogsTable(conversion);
    conversion = await createDataPoolTable(conversion);
    conversion = await loadStructureToMigrate(conversion);

    // Explicit per-table stage (managed by main.ts):
    // for each table execute "createTable + prepareDataChunks" with bounded concurrency.
    const haveTablesLoaded: boolean = await migrationStateManager.get(conversion, 'tables_loaded');
    const dataPoolTableNotEmpty: boolean = await isDataPoolTableNotEmpty(conversion);
    const haveDataChunksProcessed: boolean = haveTablesLoaded || dataPoolTableNotEmpty;
    const skipPgTableCreation: boolean = dataPoolTableNotEmpty;

    if (dataPoolTableNotEmpty) {
      await log(
        conversion,
        '	--[Main] Skip CREATE TABLE stage because data-pool table already contains records.',
      );
    }

    await runTableSubtasksInWorkerThreads(
      conversion,
      haveDataChunksProcessed,
      skipPgTableCreation,
    );

    await migrationStateManager.set(conversion, 'tables_loaded');
    conversion = await readDataPool(conversion);
    conversion = await DataPipeManager.runDataPipe(conversion);
    conversion = await decodeBinaryData(conversion);
    conversion = await processConstraints(conversion);
    conversion = await dropDataPoolTable(conversion);
    conversion = await dropStateLogsTable(conversion);
    conversion = await DbAccess.closeConnectionPools(conversion);
    await generateReport(conversion);
  } catch (error) {
    console.log(`\t--[Main] error: ${error}`);
  }
};

void runMigration();
