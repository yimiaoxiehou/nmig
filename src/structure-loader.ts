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
import DbAccess from './db-access';
import { log } from './fs-ops';
import Conversion from './conversion';
import * as extraConfigProcessor from './extra-config-processor';
import { DBAccessQueryParams, DBAccessQueryResult, DBVendors } from './types';

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
 * Loads source tables and views metadata, that need to be migrated.
 * Note, table processing subtasks are orchestrated by main.ts.
 */
export default async (conversion: Conversion): Promise<Conversion> => {
  const logTitle = 'StructureLoader::default';
  await setMySqlVersion(conversion);
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
  let tablesCnt = 0;
  let viewsCnt = 0;

  result.data.forEach((row: any) => {
    let relationName: string = row[`Tables_in_${conversion._mySqlDbName}`];

    if (row.Table_type === 'BASE TABLE' && conversion._excludeTables.indexOf(relationName) === -1) {
      relationName = extraConfigProcessor.getTableName(conversion, relationName, false);
      conversion._tablesToMigrate.push(relationName);

      conversion._dicTables.set(relationName, {
        tableLogPath: `${conversion._logsDirPath}/${relationName}.log`,
        arrTableColumns: [],
      });

      tablesCnt++;
    } else if (row.Table_type === 'VIEW') {
      conversion._viewsToMigrate.push(relationName);
      viewsCnt++;
    }
  });

  const message = `\t--[${logTitle}] Source DB structure is loaded...\n
        \t--[${logTitle}] Tables to migrate: ${tablesCnt}\n
        \t--[${logTitle}] Views to migrate: ${viewsCnt}`;

  await log(conversion, message);
  return conversion;
};
