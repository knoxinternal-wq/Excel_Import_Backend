/**
 * Preload all master maps used during import (in-memory lookups; no SQL joins on ingest).
 */
import {
  getCustomerTypeMasterMap,
  getSoTypeMasterMap,
  getPartyGroupingMasterMap,
  getAgentNameMasterMap,
  getRegionMasterMap,
  getPartyMasterAppMap,
} from '../masterLoaders.js';
import { getSoMasterMap } from '../soMasterLoader.js';

export async function loadImportMastersSnapshot() {
  const [
    customerTypeByParty,
    soTypeByParty,
    partyGroupingMap,
    agentNameMap,
    regionMap,
    partyMasterMap,
    soMasterMap,
  ] = await Promise.all([
    getCustomerTypeMasterMap(),
    getSoTypeMasterMap(),
    getPartyGroupingMasterMap(),
    getAgentNameMasterMap(),
    getRegionMasterMap(),
    getPartyMasterAppMap(),
    getSoMasterMap(),
  ]);
  return {
    customerTypeByParty,
    soTypeByParty,
    partyGroupingMap,
    agentNameMap,
    regionMap,
    partyMasterMap,
    soMasterMap,
  };
}
