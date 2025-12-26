// SPDX-FileCopyrightText: 2025 Contributors to the CitrineOS Project
//
// SPDX-License-Identifier: Apache-2.0
/**
 * Populate the database with a small, idempotent demo dataset.
 *
 * Notes:
 * - Uses bootstrap config (same as Server/runtime and db.sync.ts).
 * - Schema-aware: only inserts columns that exist in the current DB schema.
 * - Safe to re-run: checks for existing rows first.
 *
 * Usage:
 *   npm run populate-db
 *
 * Optional env:
 *   POPULATE_STATION_ID=00000000-0000-0000-0000-000000000001
 */
process.env.APP_ENV = process.env.APP_ENV ?? 'local'; // must be before imports that read env

import { DEFAULT_TENANT_ID, loadBootstrapConfig } from '@citrineos/base';
import { DefaultSequelizeInstance } from '@citrineos/data';
import { QueryTypes, Sequelize } from 'sequelize';

type TableDescription = Record<
  string,
  {
    type: unknown;
    allowNull?: boolean;
    defaultValue?: unknown;
    primaryKey?: boolean;
    autoIncrement?: boolean;
  }
>;

function qIdent(identifier: string): string {
  // Double-quote identifiers and escape embedded quotes
  return `"${identifier.replace(/"/g, '""')}"`;
}

function now(): Date {
  return new Date();
}

async function describeTableSafe(
  sequelize: Sequelize,
  table: string,
): Promise<TableDescription | null> {
  try {
    return (await sequelize.getQueryInterface().describeTable(table)) as TableDescription;
  } catch {
    return null;
  }
}

function pickExistingColumns(
  row: Record<string, unknown>,
  desc: TableDescription,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (desc[k]) out[k] = v;
  }
  return out;
}

async function selectOne<T extends Record<string, any>>(
  sequelize: Sequelize,
  sql: string,
  replacements: Record<string, unknown>,
): Promise<T | null> {
  const rows = (await sequelize.query(sql, {
    type: QueryTypes.SELECT,
    replacements,
  })) as T[];
  return rows[0] ?? null;
}

async function insertReturning<T extends Record<string, any>>(
  sequelize: Sequelize,
  table: string,
  row: Record<string, unknown>,
  returningCols: string[],
): Promise<T> {
  const cols = Object.keys(row);
  if (cols.length === 0) {
    throw new Error(`No columns to insert for table ${table}`);
  }

  const sql = `
    INSERT INTO ${qIdent(table)} (${cols.map(qIdent).join(', ')})
    VALUES (${cols.map((c) => `:${c}`).join(', ')})
    RETURNING ${returningCols.map(qIdent).join(', ')};
  `;

  const inserted = (await sequelize.query(sql, {
    type: QueryTypes.INSERT,
    replacements: row,
  })) as unknown as [T[], unknown];

  const insertedRows = inserted[0];
  if (!insertedRows?.[0]) {
    throw new Error(`Insert into ${table} did not return a row`);
  }
  return insertedRows[0];
}

async function setPostgisPointIfNull(args: {
  sequelize: Sequelize;
  table: string;
  idColumn: string;
  idValue: string | number;
  pointColumn: string;
  lng: number;
  lat: number;
}): Promise<boolean> {
  const { sequelize, table, idColumn, idValue, pointColumn, lng, lat } = args;

  const sql = `
    UPDATE ${qIdent(table)}
    SET ${qIdent(pointColumn)} = ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)
    WHERE ${qIdent(idColumn)} = :idValue
      AND ${qIdent(pointColumn)} IS NULL;
  `;

  const result = (await sequelize.query(sql, {
    type: QueryTypes.UPDATE,
    replacements: { lng, lat, idValue },
  })) as unknown as [unknown, number];

  // In Sequelize, UPDATE returns [result, affectedRows] for some dialects/drivers.
  const affected = result?.[1] ?? 0;
  return affected > 0;
}

async function ensureRow<T extends Record<string, any>>(args: {
  sequelize: Sequelize;
  table: string;
  uniqueWhereSql: string;
  uniqueParams: Record<string, unknown>;
  row: Record<string, unknown>;
  returningCols: string[];
}): Promise<{ row: T; created: boolean }> {
  const { sequelize, table, uniqueWhereSql, uniqueParams, row, returningCols } = args;

  const existing = await selectOne<T>(
    sequelize,
    `SELECT ${returningCols.map(qIdent).join(', ')} FROM ${qIdent(table)} WHERE ${uniqueWhereSql} LIMIT 1;`,
    uniqueParams,
  );
  if (existing) return { row: existing, created: false };

  const inserted = await insertReturning<T>(sequelize, table, row, returningCols);
  return { row: inserted, created: true };
}

async function main(): Promise<void> {
  const bootstrapConfig = loadBootstrapConfig();
  const sequelize = DefaultSequelizeInstance.getInstance(bootstrapConfig);

  // Quick connectivity check
  await sequelize.authenticate();

  const stationId = process.env.POPULATE_STATION_ID ?? '00000000-0000-0000-0000-000000000001';
  // Demo coordinates (San Francisco)
  const demoLng = -122.3949;
  const demoLat = 37.7936;

  // ---- Tenants
  const tenantsDesc = await describeTableSafe(sequelize, 'Tenants');
  if (!tenantsDesc) {
    throw new Error(
      'Table "Tenants" not found. Did you run migrations? Try: npm run migrate (or npm run sync-db for dev)',
    );
  }

  const tenantRow = pickExistingColumns(
    {
      id: DEFAULT_TENANT_ID,
      name: 'Default Tenant',
      createdAt: now(),
      updatedAt: now(),
    },
    tenantsDesc,
  );

  const tenantResult = await ensureRow<{ id: number }>({
    sequelize,
    table: 'Tenants',
    uniqueWhereSql: `${qIdent('id')} = :id`,
    uniqueParams: { id: DEFAULT_TENANT_ID },
    row: tenantRow,
    returningCols: ['id'],
  });

  // ---- TenantPartners (optional)
  const tenantPartnersDesc = await describeTableSafe(sequelize, 'TenantPartners');
  if (tenantPartnersDesc) {
    const tpRow = pickExistingColumns(
      {
        partyId: 'TST',
        countryCode: 'US',
        tenantId: DEFAULT_TENANT_ID,
        partnerProfileOCPI: null,
        createdAt: now(),
        updatedAt: now(),
      },
      tenantPartnersDesc,
    );

    await ensureRow<{ id: number }>({
      sequelize,
      table: 'TenantPartners',
      uniqueWhereSql: [
        `${qIdent('partyId')} = :partyId`,
        `${qIdent('countryCode')} = :countryCode`,
        tenantPartnersDesc.tenantId ? `${qIdent('tenantId')} = :tenantId` : '1=1',
      ].join(' AND '),
      uniqueParams: {
        partyId: 'TST',
        countryCode: 'US',
        tenantId: DEFAULT_TENANT_ID,
      },
      row: tpRow,
      returningCols: ['id'],
    });
  }

  // ---- Locations (optional)
  const locationsDesc = await describeTableSafe(sequelize, 'Locations');
  let locationId: number | null = null;
  if (locationsDesc) {
    const locRow = pickExistingColumns(
      {
        name: 'Demo Location',
        address: '1 Market St',
        city: 'San Francisco',
        postalCode: '94105',
        state: 'CA',
        country: 'US',
        publishUpstream: false,
        timeZone: 'UTC',
        tenantId: DEFAULT_TENANT_ID,
        createdAt: now(),
        updatedAt: now(),
      },
      locationsDesc,
    );

    const whereParts: string[] = [];
    const whereParams: Record<string, unknown> = { name: 'Demo Location' };
    if (locationsDesc.name) whereParts.push(`${qIdent('name')} = :name`);
    if (locationsDesc.tenantId) {
      whereParts.push(`${qIdent('tenantId')} = :tenantId`);
      whereParams.tenantId = DEFAULT_TENANT_ID;
    }
    if (whereParts.length === 0) {
      // Fallback to "createdAt IS NOT NULL" style query would be fragile; just skip.
      locationId = null;
    } else {
      const locRes = await ensureRow<{ id: number }>({
        sequelize,
        table: 'Locations',
        uniqueWhereSql: whereParts.join(' AND '),
        uniqueParams: whereParams,
        row: locRow,
        returningCols: ['id'],
      });
      locationId = locRes.row.id;

      // Populate PostGIS coordinates if the column exists and is currently NULL.
      if (locationsDesc.coordinates) {
        await setPostgisPointIfNull({
          sequelize,
          table: 'Locations',
          idColumn: 'id',
          idValue: locationId,
          pointColumn: 'coordinates',
          lng: demoLng,
          lat: demoLat,
        });
      }
    }
  }

  // ---- ChargingStations (optional)
  const stationsDesc = await describeTableSafe(sequelize, 'ChargingStations');
  if (stationsDesc) {
    const stationRow = pickExistingColumns(
      {
        id: stationId,
        isOnline: false,
        protocol: '2.0.1',
        chargePointVendor: 'CitrineOS',
        chargePointModel: 'DemoStation',
        locationId,
        tenantId: DEFAULT_TENANT_ID,
        createdAt: now(),
        updatedAt: now(),
      },
      stationsDesc,
    );

    await ensureRow<{ id: string }>({
      sequelize,
      table: 'ChargingStations',
      uniqueWhereSql: `${qIdent('id')} = :id`,
      uniqueParams: { id: stationId },
      row: stationRow,
      returningCols: ['id'],
    });

    // If the schema has station coordinates, populate them too (optional).
    if (stationsDesc.coordinates) {
      await setPostgisPointIfNull({
        sequelize,
        table: 'ChargingStations',
        idColumn: 'id',
        idValue: stationId,
        pointColumn: 'coordinates',
        lng: demoLng,
        lat: demoLat,
      });
    }
  }

  // ---- Evses (optional)
  const evsesDesc = await describeTableSafe(sequelize, 'Evses');
  let evseDbId: number | null = null;
  if (evsesDesc) {
    const evseRow = pickExistingColumns(
      {
        stationId,
        evseTypeId: 1,
        evseId: 'US*TST*E*00000001*1',
        removed: false,
        tenantId: DEFAULT_TENANT_ID,
        createdAt: now(),
        updatedAt: now(),
      },
      evsesDesc,
    );

    const whereParts: string[] = [];
    const whereParams: Record<string, unknown> = { stationId };
    if (evsesDesc.stationId) whereParts.push(`${qIdent('stationId')} = :stationId`);
    if (evsesDesc.evseTypeId) {
      whereParts.push(`${qIdent('evseTypeId')} = :evseTypeId`);
      whereParams.evseTypeId = 1;
    } else if (evsesDesc.evseId) {
      whereParts.push(`${qIdent('evseId')} = :evseId`);
      whereParams.evseId = 'US*TST*E*00000001*1';
    }

    if (whereParts.length > 0) {
      const returning = evsesDesc.id ? ['id'] : evsesDesc.databaseId ? ['databaseId'] : ['id'];
      const evseRes = await ensureRow<Record<string, any>>({
        sequelize,
        table: 'Evses',
        uniqueWhereSql: whereParts.join(' AND '),
        uniqueParams: whereParams,
        row: evseRow,
        returningCols: returning,
      });

      evseDbId = (evseRes.row as any).id ?? (evseRes.row as any).databaseId ?? null;
    }
  }

  // ---- Connectors (optional)
  const connectorsDesc = await describeTableSafe(sequelize, 'Connectors');
  if (connectorsDesc) {
    const connectorRow = pickExistingColumns(
      {
        stationId,
        connectorId: 1,
        status: 'Available',
        errorCode: 'NoError',
        timestamp: now(),
        info: 'Demo connector',
        evseId: evseDbId,
        evseTypeConnectorId: 1,
        tenantId: DEFAULT_TENANT_ID,
        createdAt: now(),
        updatedAt: now(),
      },
      connectorsDesc,
    );

    await ensureRow<{ id: number }>({
      sequelize,
      table: 'Connectors',
      uniqueWhereSql: `${qIdent('stationId')} = :stationId AND ${qIdent('connectorId')} = :connectorId`,
      uniqueParams: { stationId, connectorId: 1 },
      row: connectorRow,
      returningCols: ['id'],
    });
  }

  console.log(
    [
      '[populate-db] Done.',
      tenantResult.created
        ? `Created Default Tenant (id=${DEFAULT_TENANT_ID}).`
        : `Default Tenant already exists (id=${DEFAULT_TENANT_ID}).`,
      `Demo station id: ${stationId}`,
    ].join(' '),
  );

  await sequelize.close();
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[populate-db] Failed:', err);
  process.exitCode = 1;
});
