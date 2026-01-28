import React, { ChangeEvent } from 'react';
import { InlineField, Input, FieldSet } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { ChronicleDataSourceOptions, ChronicleSecureJsonData } from './types';

interface Props extends DataSourcePluginOptionsEditorProps<ChronicleDataSourceOptions, ChronicleSecureJsonData> {}

export function ConfigEditor(props: Props) {
  const { onOptionsChange, options } = props;
  const { jsonData } = options;

  const onUrlChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      url: event.target.value,
    });
  };

  const onTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      jsonData: {
        ...jsonData,
        timeout: parseInt(event.target.value, 10) || 30,
      },
    });
  };

  return (
    <FieldSet label="Chronicle Connection">
      <InlineField label="URL" labelWidth={14} tooltip="Chronicle HTTP API endpoint">
        <Input
          value={options.url || ''}
          onChange={onUrlChange}
          placeholder="http://localhost:8086"
          width={40}
        />
      </InlineField>

      <InlineField label="Timeout" labelWidth={14} tooltip="Request timeout in seconds">
        <Input
          type="number"
          value={jsonData?.timeout || 30}
          onChange={onTimeoutChange}
          placeholder="30"
          width={10}
        />
      </InlineField>
    </FieldSet>
  );
}
