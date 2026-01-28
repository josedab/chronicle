import {
  DataSourceInstanceSettings,
  CoreApp,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  MutableDataFrame,
  FieldType,
} from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';

import { ChronicleQuery, ChronicleDataSourceOptions } from './types';

export class DataSource extends DataSourceApi<ChronicleQuery, ChronicleDataSourceOptions> {
  baseUrl: string;

  constructor(instanceSettings: DataSourceInstanceSettings<ChronicleDataSourceOptions>) {
    super(instanceSettings);
    this.baseUrl = instanceSettings.url || '';
  }

  async query(options: DataQueryRequest<ChronicleQuery>): Promise<DataQueryResponse> {
    const { range } = options;
    const from = range!.from.valueOf() * 1000000; // Convert to nanoseconds
    const to = range!.to.valueOf() * 1000000;

    const promises = options.targets.map(async (target) => {
      if (target.hide) {
        return null;
      }

      const response = await this.doRequest(target, from, to);
      return this.transformResponse(response, target.refId);
    });

    const data = await Promise.all(promises);
    return { data: data.filter((d) => d !== null) as MutableDataFrame[] };
  }

  async doRequest(query: ChronicleQuery, start: number, end: number): Promise<any> {
    const requestBody = query.rawQuery
      ? { query: query.queryText }
      : {
          metric: query.metric,
          tags: query.tags || {},
          start,
          end,
          aggregation: query.aggregation
            ? {
                function: query.aggregation,
                window: query.window || '1m',
              }
            : undefined,
        };

    const result = await getBackendSrv().post(`${this.baseUrl}/query`, requestBody);
    return result;
  }

  transformResponse(response: any, refId: string): MutableDataFrame {
    const frame = new MutableDataFrame({
      refId,
      fields: [
        { name: 'time', type: FieldType.time },
        { name: 'value', type: FieldType.number },
      ],
    });

    if (response?.points) {
      for (const point of response.points) {
        frame.add({
          time: point.timestamp / 1000000, // Convert nanoseconds to milliseconds
          value: point.value,
        });
      }
    }

    return frame;
  }

  async testDatasource(): Promise<any> {
    try {
      await getBackendSrv().get(`${this.baseUrl}/health`);
      return {
        status: 'success',
        message: 'Successfully connected to Chronicle',
      };
    } catch (error) {
      return {
        status: 'error',
        message: `Failed to connect: ${error}`,
      };
    }
  }

  getDefaultQuery(app: CoreApp): Partial<ChronicleQuery> {
    return {
      metric: '',
      rawQuery: false,
      queryText: 'SELECT mean(value) FROM metric_name GROUP BY time(1m)',
    };
  }
}
