/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Button,
  FormGroup,
  HTMLSelect,
  Icon,
  InputGroup,
  Intent,
  Radio,
  RadioGroup,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Tooltip2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { useState } from 'react';

import { DestinationInfo } from '../../../talaria-models';

import './destination-form.scss';

interface DestinationFormProps {
  className?: string;
  existingTables: string[];
  destinationInfo: DestinationInfo;
  changeDestinationInfo(destinationInfo: DestinationInfo): void;
}

export const DestinationForm = React.memo(function DestinationForm(props: DestinationFormProps) {
  const { className, existingTables, destinationInfo, changeDestinationInfo } = props;

  const [tableSearch, setTableSearch] = useState<string>('');

  return (
    <div className={classNames('destination-form', className)}>
      <RadioGroup
        selectedValue={destinationInfo.mode}
        onChange={e => {
          changeDestinationInfo({ ...destinationInfo, mode: (e.target as any).value });
        }}
      >
        <Radio label="Create new table" value="new" />
        <Radio label="Append to table" value="append" />
        <Radio label="Replace table" value="replace" />
      </RadioGroup>
      {destinationInfo.mode === 'new' && (
        <FormGroup label="Table name">
          <InputGroup
            value={destinationInfo.table}
            onChange={e => {
              changeDestinationInfo({ ...destinationInfo, table: (e.target as any).value });
            }}
            placeholder="Choose a name"
            rightElement={
              existingTables.includes(destinationInfo.table) ? (
                <Tooltip2 content="Table name already exists">
                  <Button icon={IconNames.DELETE} intent={Intent.DANGER} minimal />
                </Tooltip2>
              ) : undefined
            }
          />
        </FormGroup>
      )}
      {destinationInfo.mode === 'append' && (
        <>
          <FormGroup label="Choose a table">
            <InputGroup
              value={tableSearch}
              onChange={e => {
                setTableSearch(e.target.value);
              }}
              placeholder="Search"
            />
          </FormGroup>
          <RadioGroup
            className="table-radios"
            selectedValue={destinationInfo.table}
            onChange={e => {
              changeDestinationInfo({ ...destinationInfo, table: (e.target as any).value });
            }}
          >
            {existingTables
              .filter(t => t.includes(tableSearch))
              .map(table => (
                <Radio key={table} value={table}>
                  <Icon className="table-icon" icon={IconNames.TH} />
                  {table}
                </Radio>
              ))}
          </RadioGroup>
        </>
      )}
      {destinationInfo.mode === 'replace' && (
        <FormGroup label="Table name">
          <HTMLSelect
            fill
            value={destinationInfo.table}
            onChange={e => {
              changeDestinationInfo({ ...destinationInfo, table: (e.target as any).value });
            }}
          >
            {existingTables.map(table => (
              <option key={table} value={table}>
                {table}
              </option>
            ))}
          </HTMLSelect>
        </FormGroup>
      )}
    </div>
  );
});
