import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { ChronicleQuery, ChronicleDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, ChronicleQuery, ChronicleDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
