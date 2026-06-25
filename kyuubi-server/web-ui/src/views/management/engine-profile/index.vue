<!--
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
-->
<template>
  <el-card :body-style="{ padding: '10px 14px' }" class="filter_card">
    <header>
      <el-space class="search-box">
        <el-select
          v-model="searchParam.sharelevel"
          :placeholder="$t('share_level')"
          clearable
          style="width: 210px"
          @change="getList">
          <el-option
            v-for="item in getShareLevel()"
            :key="item"
            :label="item"
            :value="item" />
        </el-select>
        <el-input
          v-model="searchParam['hive.server2.proxy.user']"
          :placeholder="$t('user')"
          style="width: 210px"
          @keyup.enter="getList" />
        <el-button type="primary" icon="Search" @click="getList" />
        <el-button type="primary" icon="Refresh" @click="getList">
          {{ $t('refresh') }}
        </el-button>
      </el-space>
    </header>
  </el-card>
  <el-card class="table-container">
    <el-table v-loading="loading" :data="tableData" style="width: 100%">
      <el-table-column type="expand">
        <template #default="scope">
          <el-table :data="scope.row.engines" style="width: 100%">
            <el-table-column
              prop="instance"
              :label="$t('engine_address')"
              min-width="25%" />
            <el-table-column :label="$t('engine_id')" min-width="25%">
              <template #default="inner">
                <span>{{
                  inner.row.attributes &&
                  inner.row.attributes['kyuubi.engine.id']
                    ? inner.row.attributes['kyuubi.engine.id']
                    : '-'
                }}</span>
              </template>
            </el-table-column>
            <el-table-column
              prop="sharelevel"
              :label="$t('share_level')"
              min-width="15%" />
            <el-table-column
              prop="user"
              :label="$t('user')"
              min-width="15%" />
            <el-table-column
              fixed="right"
              :label="$t('operation.text')"
              width="80">
              <template #default="inner">
                <el-popconfirm
                  :title="$t('operation.delete_confirm')"
                  @confirm="handleDeleteEngine(inner.row)">
                  <template #reference>
                    <span>
                      <el-tooltip
                        effect="dark"
                        :content="$t('operation.delete')"
                        placement="top">
                        <template #default>
                          <el-button type="danger" icon="Delete" circle />
                        </template>
                      </el-tooltip>
                    </span>
                  </template>
                </el-popconfirm>
              </template>
            </el-table-column>
          </el-table>
        </template>
      </el-table-column>
      <el-table-column
        prop="profile"
        :label="$t('engine_profile')"
        min-width="20%" />
      <el-table-column
        prop="engineType"
        :label="$t('engine_type')"
        min-width="18%" />
      <el-table-column
        prop="version"
        :label="$t('version')"
        min-width="14%" />
      <el-table-column
        prop="instanceCount"
        :label="$t('instance_count')"
        min-width="14%" />
      <el-table-column :label="$t('status')" min-width="14%">
        <template #default="scope">
          <el-tag :type="scope.row.status === 'RUNNING' ? 'success' : 'info'">
            {{ scope.row.status }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column
        fixed="right"
        :label="$t('operation.text')"
        width="120">
        <template #default="scope">
          <el-space wrap>
            <el-tooltip
              effect="dark"
              :content="$t('refresh')"
              placement="top">
              <el-button
                type="primary"
                icon="Refresh"
                circle
                @click="getList" />
            </el-tooltip>
            <el-popconfirm
              :title="$t('operation.delete_confirm')"
              @confirm="handleKillProfile(scope.row)">
              <template #reference>
                <span>
                  <el-tooltip
                    effect="dark"
                    :content="$t('operation.delete')"
                    placement="top">
                    <template #default>
                      <el-button
                        type="danger"
                        icon="Delete"
                        circle
                        :disabled="scope.row.instanceCount === 0" />
                    </template>
                  </el-tooltip>
                </span>
              </template>
            </el-popconfirm>
          </el-space>
        </template>
      </el-table-column>
    </el-table>
  </el-card>
</template>

<script lang="ts" setup>
  import { reactive } from 'vue'
  import { getEnginesByProfile, deleteEngine } from '@/api/engine'
  import { IEngineProfileSearch } from '@/api/engine/types'
  import { useTable } from '@/utils/use-table'
  import { ElMessage } from 'element-plus'
  import { useI18n } from 'vue-i18n'
  import { getShareLevel } from '@/utils/engine'

  const { t } = useI18n()
  const { tableData, loading, getList: _getList } = useTable()
  // default search params
  const searchParam: IEngineProfileSearch = reactive({
    sharelevel: 'USER',
    'hive.server2.proxy.user': 'anonymous'
  })
  const getList = () => {
    _getList(getEnginesByProfile, searchParam)
  }
  const init = () => {
    getList()
  }

  function deleteOneEngine(row: any) {
    return deleteEngine({
      type: row?.engineType,
      sharelevel: row?.sharelevel,
      'hive.server2.proxy.user': row?.user,
      subdomain: row?.subdomain
    })
  }

  function handleDeleteEngine(row: any) {
    deleteOneEngine(row)
      .then(() => {
        ElMessage({
          message: t('delete_succeeded', { name: 'engine' }),
          type: 'success'
        })
      })
      .catch(() => {
        ElMessage({
          message: t('delete_failed', { name: 'engine' }),
          type: 'error'
        })
      })
      .finally(() => {
        getList()
      })
  }

  function handleKillProfile(row: any) {
    const engines = row?.engines || []
    Promise.allSettled(engines.map((engine: any) => deleteOneEngine(engine)))
      .then((results) => {
        const failed = results.filter((r) => r.status === 'rejected').length
        if (failed === 0) {
          ElMessage({
            message: t('delete_succeeded', { name: 'engine' }),
            type: 'success'
          })
        } else {
          ElMessage({
            message: t('delete_failed', { name: 'engine' }),
            type: 'error'
          })
        }
      })
      .finally(() => {
        getList()
      })
  }

  init()
  // export for test
  defineExpose({
    handleKillProfile
  })
</script>

<style scoped lang="scss">
  header {
    display: flex;
    justify-content: flex-end;
  }
  .filter_card {
    margin-bottom: 10px;
  }
</style>
