import React, { ChangeEvent } from 'react';
import { InlineField, Input, Select, Switch, TextArea } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from './datasource';
import { ChronicleDataSourceOptions, ChronicleQuery } from './types';

type Props = QueryEditorProps<DataSource, ChronicleQuery, ChronicleDataSourceOptions>;

const aggregationOptions: Array<SelectableValue<string>> = [
  { label: 'None', value: '' },
  { label: 'Mean', value: 'mean' },
  { label: 'Sum', value: 'sum' },
  { label: 'Count', value: 'count' },
  { label: 'Min', value: 'min' },
  { label: 'Max', value: 'max' },
  { label: 'Stddev', value: 'stddev' },
];

const windowOptions: Array<SelectableValue<string>> = [
  { label: '1 second', value: '1s' },
  { label: '10 seconds', value: '10s' },
  { label: '1 minute', value: '1m' },
  { label: '5 minutes', value: '5m' },
  { label: '15 minutes', value: '15m' },
  { label: '1 hour', value: '1h' },
  { label: '1 day', value: '1d' },
];

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const onMetricChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange({ ...query, metric: event.target.value });
  };

  const onAggregationChange = (option: SelectableValue<string>) => {
    onChange({ ...query, aggregation: option.value });
    onRunQuery();
  };

  const onWindowChange = (option: SelectableValue<string>) => {
    onChange({ ...query, window: option.value });
    onRunQuery();
  };

  const onRawQueryToggle = () => {
    onChange({ ...query, rawQuery: !query.rawQuery });
  };

  const onQueryTextChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onChange({ ...query, queryText: event.target.value });
  };

  const { metric, aggregation, window, rawQuery, queryText } = query;

  return (
    <div>
      <InlineField label="Raw Query Mode" tooltip="Enable SQL-like query input">
        <Switch value={rawQuery} onChange={onRawQueryToggle} />
      </InlineField>

      {rawQuery ? (
        <InlineField label="Query" labelWidth={14} grow>
          <TextArea
            value={queryText || ''}
            onChange={onQueryTextChange}
            placeholder="SELECT mean(value) FROM cpu WHERE host='server1' GROUP BY time(5m)"
            rows={3}
            onBlur={onRunQuery}
          />
        </InlineField>
      ) : (
        <>
          <InlineField label="Metric" labelWidth={14} tooltip="The metric name to query">
            <Input
              value={metric || ''}
              onChange={onMetricChange}
              placeholder="cpu_usage"
              onBlur={onRunQuery}
              width={30}
            />
          </InlineField>

          <InlineField label="Aggregation" labelWidth={14}>
            <Select
              options={aggregationOptions}
              value={aggregation}
              onChange={onAggregationChange}
              width={20}
            />
          </InlineField>

          {aggregation && (
            <InlineField label="Window" labelWidth={14}>
              <Select
                options={windowOptions}
                value={window || '1m'}
                onChange={onWindowChange}
                width={20}
              />
            </InlineField>
          )}
        </>
      )}
    </div>
  );
}
