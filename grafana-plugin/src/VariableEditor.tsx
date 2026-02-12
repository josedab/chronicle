import React, { useEffect, useState } from 'react';
import { InlineField, Input, Select } from '@grafana/ui';
import { SelectableValue } from '@grafana/data';
import { DataSource } from './datasource';

interface Props {
  datasource: DataSource;
  query: { metric?: string; tag?: string };
  onChange: (query: { metric?: string; tag?: string }) => void;
}

const variableTypeOptions: Array<SelectableValue<string>> = [
  { label: 'Metric names', value: 'metrics' },
  { label: 'Tag values', value: 'tag_values' },
];

export function VariableEditor({ datasource, query, onChange }: Props) {
  const [variableType, setVariableType] = useState<string>(query.tag ? 'tag_values' : 'metrics');
  const [metrics, setMetrics] = useState<string[]>([]);

  useEffect(() => {
    datasource.getMetrics().then(setMetrics).catch(() => setMetrics([]));
  }, [datasource]);

  return (
    <div>
      <InlineField label="Variable type" labelWidth={20}>
        <Select
          options={variableTypeOptions}
          value={variableType}
          onChange={(v) => setVariableType(v.value || 'metrics')}
          width={30}
        />
      </InlineField>

      {variableType === 'metrics' && (
        <InlineField label="Metric filter" labelWidth={20}>
          <Input
            value={query.metric || ''}
            onChange={(e) => onChange({ ...query, metric: e.currentTarget.value })}
            placeholder="Optional regex filter"
            width={40}
          />
        </InlineField>
      )}

      {variableType === 'tag_values' && (
        <>
          <InlineField label="Metric" labelWidth={20}>
            <Select
              options={metrics.map((m) => ({ label: m, value: m }))}
              value={query.metric}
              onChange={(v) => onChange({ ...query, metric: v.value })}
              width={30}
              allowCustomValue
            />
          </InlineField>
          <InlineField label="Tag key" labelWidth={20}>
            <Input
              value={query.tag || ''}
              onChange={(e) => onChange({ ...query, tag: e.currentTarget.value })}
              placeholder="e.g., host, region"
              width={30}
            />
          </InlineField>
        </>
      )}
    </div>
  );
}
