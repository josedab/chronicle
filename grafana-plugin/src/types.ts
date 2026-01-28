import { DataSourceJsonData, DataQuery } from '@grafana/data';

export interface ChronicleQuery extends DataQuery {
  metric?: string;
  tags?: Record<string, string>;
  aggregation?: string;
  window?: string;
  rawQuery?: boolean;
  queryText?: string;
}

export interface ChronicleDataSourceOptions extends DataSourceJsonData {
  url?: string;
  timeout?: number;
}

export interface ChronicleSecureJsonData {
  apiKey?: string;
}
