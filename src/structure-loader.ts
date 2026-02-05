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

import DbAccess from './db-access';
import { log } from './fs-ops';
import Conversion from './conversion';
import { createTable } from './table-processor';
import prepareDataChunks from './data-chunks-processor';
import * as migrationStateManager from './migration-state-manager';
import * as extraConfigProcessor from './extra-config-processor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

type SourceRelation = {
  relationName: string;
  relationType: string;
};

/**
 * Processes current table before data loading.
 */
const processTableBeforeDataLoading = async (
  conversion: Conversion,
  tableName: string,
  stateLog: boolean,
): Promise<void> => {
  await createTable(conversion, tableName);
  await prepareDataChunks(conversion, tableName, stateLog);
};

/**
 * Resolves table-subtasks parallelism level.
 * Number is limited by configured DB pools and available CPU cores.
 */
const getTableSubtasksConcurrency = (conversion: Conversion, tablesCnt: number): number => {
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
      tablesCnt,
    ),
  );
};

/**
 * Executes table subtasks (create table + prepare data chunk) with bounded concurrency.
 */
const processTablesAsSubtasks = async (
  conversion: Conversion,
  tableNames: string[],
  haveTablesLoaded: boolean,
): Promise<void> => {
  const queue: string[] = [...tableNames];
  const workersCnt = getTableSubtasksConcurrency(conversion, tableNames.length);
  const logTitle = 'StructureLoader::processTablesAsSubtasks';

  await log(
    conversion,
    `\t--[${logTitle}] Running per-table subtasks with concurrency: ${workersCnt}`,
  );

  const worker = async (): Promise<void> => {
    while (queue.length !== 0) {
      const tableName = queue.shift();

      if (!tableName) {
        return;
      }

      await processTableBeforeDataLoading(conversion, tableName, haveTablesLoaded);
    }
  };

  const workers: Promise<void>[] = [];
  for (let i = 0; i < workersCnt; ++i) {
    workers.push(worker());
  }

  await Promise.all(workers);
};

/**
 * Retrieves the source db (MySQL) version.
 */
const setMySqlVersion = async (conversion: Conversion): Promise<void> => {
  const params: DBAccessQueryParams = {
    conversion: conversion,
    caller: 'StructureLoader::setMySqlVersion',
    sql: 'SELECT VERSION() AS mysql_version;',
    vendor: DBVendors.MYSQL,
    processExitOnError: false,
    shouldReturnClient: false,
  };

  const result: DBAccessQueryResult = await DbAccess.query(params);

  if (result.error) {
    return;
  }

  const arrVersion: string[] = result.data[0].mysql_version.split('.');
  const majorVersion: string = arrVersion[0];
  const minorVersion: string = arrVersion.slice(1).join('');
  conversion._mysqlVersion = `${majorVersion}.${minorVersion}`;
};

/**
 * Loads source tables and views, that need to be migrated.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
  const logTitle = 'StructureLoader::default';
  await setMySqlVersion(conversion);
  const haveTablesLoaded: boolean = await migrationStateManager.get(conversion, 'tables_loaded');
  let sql = `SELECT TABLE_NAME as 'Tables_in_${conversion._mySqlDbName}', TABLE_TYPE as 'Table_type' FROM information_schema.TABLES WHERE 
    TABLE_SCHEMA = '${conversion._mySqlDbName}'`;

  if (conversion._includeTables.length !== 0) {
    const tablesToInclude: string = conversion._includeTables
      .map((table: string): string => `'${table}'`)
      .join(',');

    sql += ` AND TABLE_NAME IN (${tablesToInclude})`;
  }

  if (conversion._excludeTables.length !== 0) {
    const tablesToExclude: string = conversion._excludeTables
      .map((table: string): string => `'${table}'`)
      .join(',');

    sql += ` AND TABLE_NAME NOT IN (${tablesToExclude})`;
  }

  if (conversion._excludeTableLike !== '') {
    sql += ` AND TABLE_NAME NOT LIKE '${conversion._excludeTableLike}'`;
  }
  if (conversion._includeTableLike !== '') {
    sql += ` AND TABLE_NAME LIKE '${conversion._includeTableLike}'`;
  }

  if (conversion._limit > 0) {
    sql += ` LIMIT ${conversion._limit} OFFSET ${conversion._offset}`;
  }

  const params: DBAccessQueryParams = {
    conversion: conversion,
    caller: logTitle,
    sql: `${sql};`,
    vendor: DBVendors.MYSQL,
    processExitOnError: true,
    shouldReturnClient: false,
  };

  const result: DBAccessQueryResult = await DbAccess.query(params);
  const sourceRelations: SourceRelation[] = result.data.map((row: any) => ({
    relationName: row[`Tables_in_${conversion._mySqlDbName}`],
    relationType: row.Table_type,
  }));

  const tablesToProcess: string[] = [];
  let viewsCnt = 0;

  sourceRelations.forEach((relation: SourceRelation) => {
    let relationName: string = relation.relationName;

    if (
      relation.relationType === 'BASE TABLE' &&
      conversion._excludeTables.indexOf(relationName) === -1
    ) {
      relationName = extraConfigProcessor.getTableName(conversion, relationName, false);
      conversion._tablesToMigrate.push(relationName);

      conversion._dicTables.set(relationName, {
        tableLogPath: `${conversion._logsDirPath}/${relationName}.log`,
        arrTableColumns: [],
      });

      tablesToProcess.push(relationName);
    } else if (relation.relationType === 'VIEW') {
      conversion._viewsToMigrate.push(relationName);
      viewsCnt++;
    }
  });

  const message = `\t--[${logTitle}] Source DB structure is loaded...\n
        \t--[${logTitle}] Tables to migrate: ${tablesToProcess.length}\n
        \t--[${logTitle}] Views to migrate: ${viewsCnt}`;

  await log(conversion, message);
  await processTablesAsSubtasks(conversion, tablesToProcess, haveTablesLoaded);
  await migrationStateManager.set(conversion, 'tables_loaded');
  return conversion;
};
