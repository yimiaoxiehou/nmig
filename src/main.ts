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
import { createDataPoolTable, readDataPool } from './data-pool-manager';
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
