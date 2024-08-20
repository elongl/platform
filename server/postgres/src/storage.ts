//
// Copyright © 2024 Hardcore Engineering Inc.
//
// Licensed under the Eclipse Public License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License. You may
// obtain a copy of the License at https://www.eclipse.org/legal/epl-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//

import core, {
  type AttachedDoc,
  type Class,
  type Doc,
  type DocumentQuery,
  type DocumentUpdate,
  type Domain,
  DOMAIN_MODEL,
  DOMAIN_TX,
  type FindResult,
  groupByArray,
  type Hierarchy,
  isOperator,
  type Lookup,
  type MeasureContext,
  type ModelDb,
  type ObjQueryType,
  type Projection,
  type Ref,
  type ReverseLookups,
  type SortingQuery,
  type StorageIterator,
  toFindResult,
  type Tx,
  type TxCollectionCUD,
  type TxCreateDoc,
  type TxCUD,
  type TxMixin,
  TxProcessor,
  type TxRemoveDoc,
  type TxResult,
  type TxUpdateDoc,
  type WithLookup,
  type WorkspaceId
} from '@hcengineering/core'
import {
  estimateDocSize,
  updateHashForDoc,
  type DbAdapter,
  type DbAdapterHandler,
  type DomainHelperOperations,
  type ServerFindOptions,
  type StorageAdapter,
  type TxAdapter
} from '@hcengineering/server-core'
import { createHash } from 'crypto'
import { type PoolClient } from 'pg'
import {
  convertDoc,
  createTable,
  DBCollectionHelper,
  docFields,
  escapeBackticks,
  getDBClient,
  getUpdateValue,
  isDataField,
  type JoinProps,
  parseDoc,
  parseDocWithProjection,
  type PostgresClientReference,
  retryTxn,
  translateDomain
} from './utils'

abstract class PostgresAdapterBase implements DbAdapter {
  protected readonly _helper: DBCollectionHelper
  protected readonly tableFields = new Map<string, string[]>()
  protected readonly retryTxn = async (fn: (client: PoolClient) => Promise<any>): Promise<any> => {
    await retryTxn(this.client, fn)
  }

  constructor (
    protected readonly client: PoolClient,
    protected readonly refClient: PostgresClientReference,
    protected readonly workspaceId: WorkspaceId,
    protected readonly hierarchy: Hierarchy,
    protected readonly modelDb: ModelDb
  ) {
    this._helper = new DBCollectionHelper(this.client, this.workspaceId)
  }

  helper (): DomainHelperOperations {
    return this._helper
  }

  on?: ((handler: DbAdapterHandler) => void) | undefined

  abstract init (): Promise<void>

  async close (): Promise<void> {
    this.refClient.close()
  }

  async findAll<T extends Doc>(
    ctx: MeasureContext,
    _class: Ref<Class<T>>,
    query: DocumentQuery<T>,
    options?: ServerFindOptions<T>
  ): Promise<FindResult<T>> {
    try {
      const domain = translateDomain(options?.domain ?? this.hierarchy.getDomain(_class))
      const sqlChunks: string[] = []
      const joins = this.buildJoin(_class, options?.lookup)
      const select = `SELECT ${this.getProjection(ctx, _class, domain, options?.projection, joins)} FROM ${domain}`
      if (joins.length > 0) {
        sqlChunks.push(this.buildJoinString(joins))
      }
      sqlChunks.push(`WHERE ${this.buildQuery(_class, domain, query, joins, options)}`)

      let total = options?.total === true ? 0 : -1
      if (options?.total === true) {
        const totalReq = `SELECT COUNT(${domain}._id) as count FROM ${domain}`
        const totalSql = [totalReq, ...sqlChunks].join(' ')
        const totalResult = await this.client.query(totalSql)
        const parsed = Number.parseInt(totalResult.rows[0]?.count ?? '')
        total = Number.isNaN(parsed) ? 0 : parsed
      }
      if (options?.sort !== undefined) {
        sqlChunks.push(this.buildOrder(_class, domain, options.sort, joins))
      }
      if (options?.limit !== undefined) {
        sqlChunks.push(`LIMIT ${options.limit}`)
      }

      const finalSql: string = [select, ...sqlChunks].join(' ')
      const result = await this.client.query(finalSql)
      if (options?.lookup === undefined) {
        return toFindResult(
          result.rows.map((p) => parseDocWithProjection(p, options?.projection)),
          total
        )
      } else {
        const res = this.parseLookup<T>(result.rows, joins, options?.projection)
        return toFindResult(res, total)
      }
    } catch (err) {
      console.log(err)
      throw err
    }
  }

  private parseLookup<T extends Doc>(
    rows: any[],
    joins: JoinProps[],
    projection: Projection<T> | undefined
  ): WithLookup<T>[] {
    const map = new Map<Ref<T>, WithLookup<T>>()
    const modelJoins: JoinProps[] = []
    const reverseJoins: JoinProps[] = []
    const simpleJoins: JoinProps[] = []
    for (const join of joins) {
      if (join.table === DOMAIN_MODEL) {
        modelJoins.push(join)
      } else if (join.isReverse) {
        reverseJoins.push(join)
      } else {
        simpleJoins.push(join)
      }
    }
    for (const row of rows) {
      /* eslint-disable @typescript-eslint/consistent-type-assertions */
      let doc: WithLookup<T> = map.get(row._id) ?? ({ _id: row._id, $lookup: {} } as WithLookup<T>)
      const lookup: Record<string, any> = doc.$lookup as Record<string, any>
      let joinIndex: number | undefined
      let skip = false
      try {
        for (const column in row) {
          if (column.startsWith('reverse_lookup_')) {
            const join = reverseJoins.find((j) => j.toAlias === column)
            if (join === undefined) {
              continue
            }
            const res = this.getLookupValue(join.path, lookup, false)
            if (res === undefined) continue
            const { obj, key } = res

            if (row[column] != null) {
              const parsed = row[column].map(parseDoc)
              obj[key] = parsed
            }
          } else if (column.startsWith('lookup_')) {
            const keys = column.split('_')
            let key = keys[keys.length - 1]
            if (keys[keys.length - 2] === '') {
              key = '_' + key
            }

            if (key === 'workspaceId') {
              continue
            }

            if (key === '_id') {
              if (row[column] === null) {
                skip = true
                continue
              }
              joinIndex = joinIndex === undefined ? 0 : ++joinIndex
              skip = false
            }

            if (skip) {
              continue
            }

            const join = simpleJoins[joinIndex ?? 0]

            const res = this.getLookupValue(join.path, lookup)
            if (res === undefined) continue
            const { obj, key: p } = res

            if (key === 'data') {
              obj[p] = { ...obj[p], ...row[column] }
            } else {
              if (key === 'attachedTo' && row[column] === 'NULL') {
                continue
              } else {
                obj[p][key] = row[column] === 'NULL' ? null : row[column]
              }
            }
          } else {
            joinIndex = undefined
            if (!map.has(row._id)) {
              if (column === 'workspaceId') {
                continue
              }
              if (column === 'data') {
                const data = row[column]
                if (projection !== undefined) {
                  if (projection !== undefined) {
                    for (const key in data) {
                      if (!Object.prototype.hasOwnProperty.call(projection, key) || (projection as any)[key] === 0) {
                        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                        delete data[key]
                      }
                    }
                  }
                }
                doc = { ...doc, ...data }
              } else {
                if (column === 'createdOn' || column === 'modifiedOn') {
                  const val = Number.parseInt(row[column])
                  ;(doc as any)[column] = Number.isNaN(val) ? null : val
                } else {
                  ;(doc as any)[column] = row[column] === 'NULL' ? null : row[column]
                }
              }
            }
          }
        }
      } catch (err) {
        console.log(err)
        throw err
      }
      for (const modelJoin of modelJoins) {
        const res = this.getLookupValue(modelJoin.path, lookup)
        if (res === undefined) continue
        const { obj, key } = res
        const val = this.getModelLookupValue<T>(doc, modelJoin, simpleJoins)
        if (val !== undefined) {
          const res = this.modelDb.findAllSync(modelJoin.toClass, {
            [modelJoin.toField]: (doc as any)[modelJoin.fromField]
          })
          obj[key] = modelJoin.isReverse ? res : res[0]
        }
      }
      map.set(row._id, doc)
    }
    return Array.from(map.values())
  }

  private getLookupValue (
    fullPath: string,
    obj: Record<string, any>,
    shouldCreate: boolean = true
  ):
    | {
      obj: any
      key: string
    }
    | undefined {
    const path = fullPath.split('.')
    for (let i = 0; i < path.length; i++) {
      const p = path[i]
      if (i > 0) {
        if (obj.$lookup === undefined) {
          obj.$lookup = {}
        }
        obj = obj.$lookup
      }

      if (obj[p] === undefined) {
        if (!shouldCreate && i < path.length - 1) {
          return
        } else {
          obj[p] = {}
        }
      }
      if (i === path.length - 1) {
        return { obj, key: p }
      }
      obj = obj[p]
    }
  }

  private getModelLookupValue<T extends Doc>(doc: WithLookup<T>, join: JoinProps, simpleJoins: JoinProps[]): any {
    if (join.fromAlias.startsWith('lookup_')) {
      const simple = simpleJoins.find((j) => j.toAlias === join.fromAlias)
      if (simple !== undefined) {
        const val = this.getLookupValue(simple.path, doc.$lookup ?? {})
        if (val !== undefined) {
          const data = val.obj[val.key]
          return data[join.fromField]
        }
      }
    } else {
      return (doc as any)[join.fromField]
    }
  }

  private buildJoinString (value: JoinProps[]): string {
    const res: string[] = []
    for (const val of value) {
      if (val.isReverse) continue
      if (val.table === DOMAIN_MODEL) continue
      res.push(
        `LEFT JOIN ${val.table} AS ${val.toAlias} ON ${val.fromAlias}.${val.fromField} = ${val.toAlias}."${val.toField}" AND ${val.toAlias}."workspaceId" = '${this.workspaceId.name}'`
      )
      if (val.classes !== undefined) {
        if (val.classes.length === 1) {
          res.push(`AND ${val.toAlias}._class = '${val.classes[0]}'`)
        }
        res.push(`AND ${val.toAlias}._class IN (${val.classes.map((c) => `'${c}'`).join(', ')})`)
      }
    }
    return res.join(' ')
  }

  private buildJoin<T extends Doc>(clazz: Ref<Class<T>>, lookup: Lookup<T> | undefined): JoinProps[] {
    const res: JoinProps[] = []
    if (lookup !== undefined) {
      this.buildJoinValue(clazz, lookup, res)
    }
    return res
  }

  private buildJoinValue<T extends Doc>(
    clazz: Ref<Class<T>>,
    lookup: Lookup<T>,
    res: JoinProps[],
    parentKey?: string,
    parentAlias?: string
  ): void {
    const baseDomain = parentAlias ?? translateDomain(this.hierarchy.getDomain(clazz))
    for (const key in lookup) {
      if (key === '_id') {
        this.getReverseLookupValue(baseDomain, lookup, res, parentKey)
        continue
      }
      const value = (lookup as any)[key]
      const _class = Array.isArray(value) ? value[0] : value
      const nested = Array.isArray(value) ? value[1] : undefined
      const domain = translateDomain(this.hierarchy.getDomain(_class))
      const tkey = domain === DOMAIN_MODEL ? key : this.transformKey(clazz, key)
      const as = `lookup_${domain}_${parentKey !== undefined ? parentKey + '_lookup_' + key : key}`
      res.push({
        isReverse: false,
        table: domain,
        path: parentKey !== undefined ? `${parentKey}.${key}` : key,
        toAlias: as,
        toField: '_id',
        fromField: tkey,
        fromAlias: baseDomain,
        toClass: _class
      })
      if (nested !== undefined) {
        this.buildJoinValue(_class, nested, res, key, as)
      }
    }
  }

  private getReverseLookupValue (
    parentDomain: string,
    lookup: ReverseLookups,
    result: JoinProps[],
    parent?: string
  ): void {
    const lid = lookup?._id ?? {}
    for (const key in lid) {
      const value = lid[key]

      let _class: Ref<Class<Doc>>
      let attr = 'attachedTo'

      if (Array.isArray(value)) {
        _class = value[0]
        attr = value[1]
      } else {
        _class = value
      }
      const domain = translateDomain(this.hierarchy.getDomain(_class))
      const desc = this.hierarchy
        .getDescendants(this.hierarchy.getBaseClass(_class))
        .filter((it) => !this.hierarchy.isMixin(it))
      const as = `reverse_lookup_${domain}_${parent !== undefined ? parent + '_lookup_' + key : key}`
      result.push({
        isReverse: true,
        table: domain,
        toAlias: as,
        toField: attr,
        classes: desc,
        path: parent !== undefined ? `${parent}.${key}` : key,
        fromAlias: parentDomain,
        toClass: _class,
        fromField: '_id'
      })
    }
  }

  private buildOrder<T extends Doc>(
    _class: Ref<Class<T>>,
    baseDomain: string,
    sort: SortingQuery<T>,
    joins: JoinProps[]
  ): string {
    const res: string[] = []
    for (const key in sort) {
      const val = sort[key]
      if (val === undefined) {
        continue
      }
      if (typeof val === 'number') {
        res.push(`${this.getKey(_class, baseDomain, key, joins)} ${val === 1 ? 'ASC' : 'DESC'}`)
      } else {
        // todo handle custom sorting
      }
    }
    return `ORDER BY ${res.join(', ')}`
  }

  private buildQuery<T extends Doc>(
    _class: Ref<Class<T>>,
    baseDomain: string,
    query: DocumentQuery<T>,
    joins: JoinProps[],
    options?: ServerFindOptions<T>
  ): string {
    const res: string[] = []
    res.push(`${baseDomain}."workspaceId" = '${this.workspaceId.name}'`)
    if (options?.skipClass !== true) {
      query._class = this.fillClass(_class, query) as any
    }
    for (const key in query) {
      if (options?.skipSpace === true && key === 'space') {
        continue
      }
      if (options?.skipClass === true && key === '_class') {
        continue
      }
      const value = query[key]
      const isDataArray = this.checkDataArray(_class, key)
      const tkey = this.getKey(_class, baseDomain, key, joins, isDataArray)
      const translated = this.translateQueryValue(tkey, value, isDataArray)
      if (translated !== undefined) {
        res.push(translated)
      }
    }
    return res.join(' AND ')
  }

  private checkDataArray<T extends Doc>(_class: Ref<Class<T>>, key: string): boolean {
    const attr = this.hierarchy.findAttribute(_class, key)
    if (attr !== undefined) {
      return attr.type._class === core.class.ArrOf
    }
    return false
  }

  private fillClass<T extends Doc>(
    _class: Ref<Class<T>>,
    query: DocumentQuery<T>
  ): ObjQueryType<Doc['_class']> | undefined {
    let value: any = query._class
    const baseClass = this.hierarchy.getBaseClass(_class)
    if (baseClass !== core.class.Doc) {
      const classes = this.hierarchy.getDescendants(baseClass).filter((it) => !this.hierarchy.isMixin(it))

      // Only replace if not specified
      if (value === undefined) {
        value = { $in: classes }
      } else if (typeof value === 'string') {
        if (!classes.includes(value as Ref<Class<T>>)) {
          value = classes.length === 1 ? classes[0] : { $in: classes }
        }
      } else if (typeof value === 'object' && value !== null) {
        let descendants: Ref<Class<Doc>>[] = classes

        if (Array.isArray(value.$in)) {
          const classesIds = new Set(classes)
          descendants = value.$in.filter((c: Ref<Class<Doc>>) => classesIds.has(c))
        }

        if (value != null && Array.isArray(value.$nin)) {
          const excludedClassesIds = new Set<Ref<Class<Doc>>>(value.$nin)
          descendants = descendants.filter((c) => !excludedClassesIds.has(c))
        }

        if (value.$ne != null) {
          descendants = descendants.filter((c) => c !== value?.$ne)
        }

        const desc = descendants.filter((it: any) => !this.hierarchy.isMixin(it as Ref<Class<Doc>>))
        value = desc.length === 1 ? desc[0] : { $in: desc }
      }

      if (baseClass !== _class) {
        // Add an mixin to be exists flag
        ;(query as any)[_class] = { $exists: true }
      }
    } else {
      // No need to pass _class in case of fixed domain search.
      return undefined
    }
    if (value?.$in?.length === 1 && value?.$nin === undefined) {
      value = value.$in[0]
    }
    return value
  }

  private getKey<T extends Doc>(
    _class: Ref<Class<T>>,
    baseDomain: string,
    key: string,
    joins: JoinProps[],
    isDataArray: boolean = false
  ): string {
    if (key.startsWith('$lookup')) {
      return this.transformLookupKey(key, joins, isDataArray)
    }
    return `${baseDomain}.${this.transformKey(_class, key, isDataArray)}`
  }

  private transformLookupKey (key: string, joins: JoinProps[], isDataArray: boolean = false): string {
    const arr = key.split('.').filter((p) => p !== '$lookup')
    const tKey = arr.pop() ?? ''
    const path = arr.join('.')
    const join = joins.find((p) => p.path === path)
    if (join === undefined) {
      throw new Error(`Can't fined join for path: ${path}`)
    }
    if (join.isReverse) {
      return `${join.toAlias}->'${tKey}'`
    }
    const res = isDataField(tKey) ? (isDataArray ? `data->'${tKey}'` : `data#>>'{${tKey}}'`) : key
    return `${join.toAlias}.${res}`
  }

  private transformKey<T extends Doc>(_class: Ref<Class<T>>, key: string, isDataArray: boolean = false): string {
    if (!isDataField(key)) return `"${key}"`
    const arr = key.split('.').filter((p) => p)
    let tKey = ''

    for (let i = 0; i < arr.length; i++) {
      const element = arr[i]
      if (element === '$lookup') {
        tKey += arr[++i] + '_lookup'
      } else {
        if (!tKey.endsWith('.') && i > 0) {
          tKey += '.'
        }
        tKey += arr[i]
        if (i !== arr.length - 1) {
          tKey += '.'
        }
      }
      // Check if key is belong to mixin class, we need to add prefix.
      tKey = this.checkMixinKey<T>(tKey, _class, isDataArray)
    }

    return isDataArray ? `data->'${tKey}'` : `data#>>'{${tKey}}'`
  }

  private checkMixinKey<T extends Doc>(key: string, _class: Ref<Class<T>>, isDataArray: boolean): string {
    if (!key.includes('.')) {
      try {
        const attr = this.hierarchy.findAttribute(_class, key)
        if (attr !== undefined && this.hierarchy.isMixin(attr.attributeOf)) {
          // It is mixin
          if (isDataArray) {
            key = `${attr.attributeOf}->${key}`
          } else {
            key = `${attr.attributeOf},${key}`
          }
        }
      } catch (err: any) {
        // ignore, if
      }
    }
    return key
  }

  private translateQueryValue (tkey: string, value: any, isDataArray: boolean): string | undefined {
    if (value === null) {
      return `${tkey} IS NULL`
    } else if (typeof value === 'object' && !Array.isArray(value)) {
      // we can have multiple criteria for one field
      const res: string[] = []
      for (const operator in value) {
        const val = value[operator]
        switch (operator) {
          case '$ne':
            res.push(`${tkey} != '${val}'`)
            break
          case '$gt':
            res.push(`${tkey} > '${val}'`)
            break
          case '$gte':
            res.push(`${tkey} >= '${val}'`)
            break
          case '$lt':
            res.push(`${tkey} < '${val}'`)
            break
          case '$lte':
            res.push(`${tkey} <= '${val}'`)
            break
          case '$in':
            res.push(
              isDataArray
                ? `${tkey} ?| array[${val.length > 0 ? val.map((v: any) => `'${v}'`).join(', ') : 'NULL'}]`
                : `${tkey} IN (${val.length > 0 ? val.map((v: any) => `'${v}'`).join(', ') : 'NULL'})`
            )
            break
          case '$nin':
            if (val.length > 0) {
              res.push(`${tkey} NOT IN (${val.map((v: any) => `'${v}'`).join(', ')})`)
            }
            break
          case '$like':
            res.push(`${tkey} ILIKE '${val}'`)
            break
          case '$exists':
            res.push(`${tkey} IS ${val === true ? 'NOT NULL' : 'NULL'}`)
            break
        }
      }
      return res.length === 0 ? undefined : res.join(' AND ')
    }
    return isDataArray
      ? `${tkey} @> '${typeof value === 'string' ? '"' + value + '"' : value}'`
      : `${tkey} = '${value}'`
  }

  private getProjectionsAliases (join: JoinProps): string[] {
    if (join.table === DOMAIN_MODEL) return []
    if (join.isReverse) {
      return [
        `(SELECT jsonb_agg(${join.toAlias}.*) FROM ${join.table} AS ${join.toAlias} WHERE ${join.fromAlias}.${join.fromField} = ${join.toAlias}."${join.toField}") AS ${join.toAlias}`
      ]
    }
    const res: string[] = []
    for (const key of [...docFields, 'data']) {
      res.push(`${join.toAlias}."${key}" as "lookup_${join.path.replaceAll('.', '_')}_${key}"`)
    }
    return res
  }

  private getProjection<T extends Doc>(
    ctx: MeasureContext,
    _class: Ref<Class<T>>,
    baseDomain: string,
    projection: Projection<T> | undefined,
    joins: JoinProps[]
  ): string | '*' {
    if (projection === undefined && joins.length === 0) return '*'
    const res: string[] = []
    let dataAdded = false
    if (projection === undefined) {
      res.push(`${baseDomain}.*`)
    } else {
      for (const key in projection) {
        if (isDataField(key)) {
          if (!dataAdded) {
            res.push('data as data')
            dataAdded = true
          }
        } else {
          res.push(`${baseDomain}."${key}" AS "${key}"`)
        }
      }
    }
    for (const join of joins) {
      res.push(...this.getProjectionsAliases(join))
    }
    return res.join(', ')
  }

  async tx (ctx: MeasureContext, ...tx: Tx[]): Promise<TxResult[]> {
    return []
  }

  async * fetchRows (query: string): AsyncGenerator<any, void, unknown> {
    const cursorQuery = `${query} DECLARE my_cursor CURSOR FOR SELECT * FROM your_table;`
    await this.client.query(cursorQuery);
  
    let fetchMore = true;
  
    while (fetchMore) {
      const result = await this.client.query('FETCH 100 FROM my_cursor;')
  
      for (const row of result.rows) {
        yield row;
      }
  
      fetchMore = result.rows.length > 0
    }
  
    await this.client.query('CLOSE my_cursor;')
  }

  find (_ctx: MeasureContext, domain: Domain, recheck?: boolean): StorageIterator {
    const ctx = _ctx.newChild('find', { domain })

    const getCursorName = () => {
      return `cursor_${translateDomain(this.workspaceId.name)}_${translateDomain(domain)}_${mode}`
    }

    let initialized: boolean = false
    let mode: 'hashed' | 'non_hashed' = 'hashed'
    let cursorName = getCursorName()
    const bulkUpdate = new Map<Ref<Doc>, string>()

    const close = async (cursorName: string) => {
      try {
        await this.client.query(`CLOSE ${cursorName}`)
        await this.client.query(`COMMIT`)
        ctx.info('Cursor closed', { cursorName })
      } catch (err) {
        ctx.error('Error while closing cursor', { cursorName, err })
      }
    }

    const init = async (projection: string, query: string) => {
      cursorName = getCursorName()
      await this.client.query(`BEGIN`)
      await this.client.query(`DECLARE ${cursorName} CURSOR FOR SELECT ${projection} FROM ${translateDomain(domain)} WHERE "workspaceId" = $1 AND ${query}`, [this.workspaceId.name])
      ctx.info('Cursor initialized', { cursorName })
    }

    const next = async (): Promise<Doc | null> => {
      const result = await this.client.query(`FETCH 1 FROM ${cursorName}`)
      if (result.rows.length === 0) {
        return null
      }
      return result.rows[0] !== undefined ? parseDoc(result.rows[0]) : null
    }

    const flush = async (flush = false): Promise<void> => {
      if (bulkUpdate.size > 1000 || flush) {
        if (bulkUpdate.size > 0) {
          await ctx.with(
            'bulk-write-find',
            {},
            async () => {
              const updates = new Map(Array.from(bulkUpdate.entries()).map((it) => [it[0], { '%hash%': it[1] }]))
              await this.update(ctx, domain, updates)
            }
          )
        }
        bulkUpdate.clear()
      }
    }

    return {
      next: async () => {
        if (!initialized) {
          await init('_id, data', `data ->> '%hash%' IS NOT NULL AND data ->> '%hash%' <> ''`)
          initialized = true
        }
        let d = await ctx.with('next', { mode }, async () => await next())
        if (d == null && mode === 'hashed') {
          await close(cursorName)
          mode = 'non_hashed'
          await init('*', `data ->> '%hash%' IS NULL OR data ->> '%hash%' = ''`)
          d = await ctx.with('next', { mode }, async () => await next())
        }
        if (d == null) {
          return undefined
        }
        let digest: string | null = (d as any)['%hash%']
        if ('%hash%' in d) {
          delete d['%hash%']
        }
        const pos = (digest ?? '').indexOf('|')
        if (digest == null || digest === '') {
          const cs = ctx.newChild('calc-size', {})
          const size = estimateDocSize(d)
          cs.end()

          const hash = createHash('sha256')
          updateHashForDoc(hash, d)
          digest = hash.digest('base64')

          bulkUpdate.set(d._id, `${digest}|${size.toString(16)}`)

          await ctx.with('flush', {}, async () => {
            await flush()
          })
          return {
            id: d._id,
            hash: digest,
            size
          }
        } else {
          return {
            id: d._id,
            hash: digest.slice(0, pos),
            size: parseInt(digest.slice(pos + 1), 16)
          }
        }
      },
      close: async () => {
        await ctx.with('flush', {}, async () => {
          await flush(true)
        })
        await close(cursorName)
        ctx.end()
      }
    }
  }

  async load (ctx: MeasureContext, domain: Domain, docs: Ref<Doc>[]): Promise<Doc[]> {
    return await ctx.with('load', { domain }, async () => {
      if (docs.length === 0) {
        return []
      }
      const res = await this.client.query(
        `SELECT * FROM ${translateDomain(domain)} WHERE _id = ANY($1) AND "workspaceId" = $2`,
        [docs, this.workspaceId.name]
      )
      return res.rows as Doc[]
    })
  }

  async upload (ctx: MeasureContext, domain: Domain, docs: Doc[]): Promise<void> {
    return await this.retryTxn(async (client) => {
      while (docs.length > 0) {
        const part = docs.splice(0, 1)
        const vals = part
          .map((doc) => {
            const d = convertDoc(doc, this.workspaceId.name)
            return `('${d._id}', '${d.workspaceId}', '${d._class}', '${d.createdBy ?? d.modifiedBy}', '${d.modifiedBy}', ${d.modifiedOn}, ${d.createdOn ?? d.modifiedOn}, '${d.space}', '${d.attachedTo ?? 'NULL'}', '${escapeBackticks(JSON.stringify(d.data))}')`
          })
          .join(', ')
        await client.query(
          `INSERT INTO ${translateDomain(domain)} (_id, "workspaceId", _class, "createdBy", "modifiedBy", "modifiedOn", "createdOn", space, "attachedTo", data) VALUES ${vals} 
          ON CONFLICT (_id, "workspaceId") DO UPDATE SET _class = EXCLUDED._class, "createdBy" = EXCLUDED."createdBy", "modifiedBy" = EXCLUDED."modifiedBy", "modifiedOn" = EXCLUDED."modifiedOn", "createdOn" = EXCLUDED."createdOn", space = EXCLUDED.space, "attachedTo" = EXCLUDED."attachedTo", data = EXCLUDED.data;`
        )
      }
    })
  }

  async clean (ctx: MeasureContext, domain: Domain, docs: Ref<Doc>[]): Promise<void> {
    await this.client.query(`DELETE FROM ${translateDomain(domain)} WHERE _id = ANY($1) AND "workspaceId" = $2`, [
      docs,
      this.workspaceId.name
    ])
  }

  async groupBy<T>(ctx: MeasureContext, domain: Domain, field: string): Promise<Set<T>> {
    try {
      const result = await ctx.with('groupBy', { domain }, async (ctx) => {
        const result = await this.client.query(
          `SELECT DISTINCT ${field} FROM ${translateDomain(domain)} WHERE "workspaceId" = $1`,
          [this.workspaceId.name]
        )
        return new Set(result.rows.map((r) => r[field]))
      })
      return result
    } catch (err) {
      ctx.error('Error while grouping by', { domain, field })
      throw err
    }
  }

  async update (ctx: MeasureContext, domain: Domain, operations: Map<Ref<Doc>, DocumentUpdate<Doc>>): Promise<void> {
    const ids = Array.from(operations.keys())
    await this.retryTxn(async (client) => {
      try {
        const res = await client.query(
          `SELECT * FROM ${translateDomain(domain)} WHERE _id = ANY($1) AND "workspaceId" = $2 FOR UPDATE`,
          [ids, this.workspaceId.name]
        )
        const docs = res.rows.map(parseDoc)
        const map = new Map(docs.map((d) => [d._id, d]))
        for (const [_id, ops] of operations) {
          const doc = map.get(_id)
          if (doc === undefined) continue
          if ((ops as any)['%hash%'] === undefined) {
            ;(ops as any)['%hash%'] = null
          }
          TxProcessor.applyUpdate(doc, ops)
          const converted = convertDoc(doc, this.workspaceId.name)
          await client.query(`UPDATE ${translateDomain(domain)} SET data = $3 WHERE _id = $1 AND "workspaceId" = $2`, [
            _id,
            this.workspaceId.name,
            converted.data
          ])
        }
      } catch (err) {
        ctx.error('Error while updating', { domain, operations, err })
        throw err
      }
    })
  }

  async insert (domain: string, docs: Doc[]): Promise<TxResult> {
    return await this.retryTxn(async (client) => {
      while (docs.length > 0) {
        const part = docs.splice(0, 1)
        const vals = part
          .map((doc) => {
            const d = convertDoc(doc, this.workspaceId.name)
            return `('${d._id}', '${d.workspaceId}', '${d._class}', '${d.createdBy ?? d.modifiedBy}', '${d.modifiedBy}', ${d.modifiedOn}, ${d.createdOn ?? d.modifiedOn}, '${d.space}', '${d.attachedTo ?? 'NULL'}', '${escapeBackticks(JSON.stringify(d.data))}')`
          })
          .join(', ')
        await client.query(
          `INSERT INTO ${translateDomain(domain)} (_id, "workspaceId", _class, "createdBy", "modifiedBy", "modifiedOn", "createdOn", space, "attachedTo", data) VALUES ${vals}`
        )
      }
    })
  }
}

class PostgresAdapter extends PostgresAdapterBase {
  async init (domains?: string[], excludeDomains?: string[]): Promise<void> {
    let resultDomains = domains ?? this.hierarchy.domains()
    if (excludeDomains !== undefined) {
      resultDomains = resultDomains.filter((it) => !excludeDomains.includes(it))
    }
    await createTable(this.client, resultDomains)
    this._helper.domains = new Set(resultDomains as Domain[])
  }

  private async process (tx: Tx): Promise<TxResult | undefined> {
    switch (tx._class) {
      case core.class.TxCreateDoc:
        return await this.txCreateDoc(tx as TxCreateDoc<Doc>)
      case core.class.TxCollectionCUD:
        return await this.txCollectionCUD(tx as TxCollectionCUD<Doc, AttachedDoc>)
      case core.class.TxUpdateDoc:
        return await this.txUpdateDoc(tx as TxUpdateDoc<Doc>)
      case core.class.TxRemoveDoc:
        await this.txRemoveDoc(tx as TxRemoveDoc<Doc>)
        break
      case core.class.TxMixin:
        return await this.txMixin(tx as TxMixin<Doc, Doc>)
      case core.class.TxApplyIf:
        return undefined
      default:
        console.error('Unknown/Unsupported operation:', tx._class, tx)
        break
    }
  }

  protected async txCollectionCUD (tx: TxCollectionCUD<Doc, AttachedDoc>): Promise<TxResult | undefined> {
    // We need update only create transactions to contain attached, attachedToClass.
    if (tx.tx._class === core.class.TxCreateDoc) {
      const createTx = tx.tx as TxCreateDoc<AttachedDoc>
      const d: TxCreateDoc<AttachedDoc> = {
        ...createTx,
        attributes: {
          ...createTx.attributes,
          attachedTo: tx.objectId,
          attachedToClass: tx.objectClass,
          collection: tx.collection
        }
      }
      return await this.txCreateDoc(d)
    }
    // We could cast since we know collection cud is supported.
    return await this.process(tx.tx as Tx)
  }

  private async txMixin (tx: TxMixin<Doc, Doc>): Promise<TxResult> {
    await this.retryTxn(async (client) => {
      const doc = await this.findDoc(tx.objectClass, tx.objectId, true)
      if (doc === undefined) return {}
      TxProcessor.updateMixin4Doc(doc, tx)
      const converted = convertDoc(doc, this.workspaceId.name)
      await client.query(
        `UPDATE ${translateDomain(this.hierarchy.getDomain(tx.objectClass))} SET "modifiedBy" = $1, "modifiedOn" = $2, data = $5 WHERE _id = $3 AND "workspaceId" = $4`,
        [tx.modifiedBy, tx.modifiedOn, tx.objectId, this.workspaceId.name, converted.data]
      )
    })
    return {}
  }

  async tx (ctx: MeasureContext, ...txes: Tx[]): Promise<TxResult[]> {
    const result: TxResult[] = []

    const h = this.hierarchy
    const byDomain = groupByArray(txes, (it) => {
      if (TxProcessor.isExtendsCUD(it._class)) {
        return h.findDomain((it as TxCUD<Doc>).objectClass)
      }
      return undefined
    })
    for (const [domain, txs] of byDomain) {
      if (domain === undefined) {
        continue
      }
      for (const tx of txs) {
        const res = await this.process(tx)
        if (res !== undefined) {
          result.push(res)
        }
      }
    }

    return result
  }

  protected async txCreateDoc (tx: TxCreateDoc<Doc>): Promise<TxResult> {
    const doc = TxProcessor.createDoc2Doc(tx)
    return await this.insert(translateDomain(this.hierarchy.getDomain(doc._class)), [doc])
  }

  protected async txUpdateDoc (tx: TxUpdateDoc<Doc>): Promise<TxResult> {
    if (isOperator(tx.operations)) {
      let doc: Doc | undefined
      await this.retryTxn(async (client) => {
        doc = await this.findDoc(tx.objectClass, tx.objectId, true)
        if (doc === undefined) return {}
        ;(tx.operations as any)['%hash%'] = null
        TxProcessor.applyUpdate(doc, tx.operations)
        const converted = convertDoc(doc, this.workspaceId.name)
        await client.query(
          `UPDATE ${translateDomain(this.hierarchy.getDomain(tx.objectClass))} SET "modifiedBy" = $1, "modifiedOn" = $2, data = $5 WHERE _id = $3 AND "workspaceId" = $4`,
          [tx.modifiedBy, tx.modifiedOn, tx.objectId, this.workspaceId.name, converted.data]
        )
      })
      if (tx.retrieve === true && doc !== undefined) {
        return { object: doc }
      }
    } else {
      await this.updateDoc(tx)
      if (tx.retrieve === true) {
        return { object: await this.findDoc(tx.objectClass, tx.objectId) }
      }
    }
    return {}
  }

  private async updateDoc<T extends Doc>(tx: TxUpdateDoc<T>): Promise<void> {
    const updates: string[] = ['"modifiedBy" = $1', '"modifiedOn" = $2']
    const { space, attachedTo, ...ops } = tx.operations as any
    if (space !== undefined) {
      updates.push(`space = ${space}`)
    }
    if (attachedTo !== undefined) {
      updates.push(`"attachedTo" = ${attachedTo !== undefined ? "'" + attachedTo + "'" : 'NULL'}`)
    }
    if (Object.keys(ops).length > 0) {
      let from = 'data'
      for (const key in ops) {
        from = `jsonb_set(${from}, '{${key}}', '${getUpdateValue(ops[key])}', true)`
      }
      updates.push(`data = ${from}`)
    }

    try {
      await this.retryTxn(async (client) => {
        await client.query(
          `UPDATE ${translateDomain(this.hierarchy.getDomain(tx.objectClass))} SET ${updates.join(', ')} WHERE _id = $3 AND "workspaceId" = $4`,
          [tx.modifiedBy, tx.modifiedOn, tx.objectId, this.workspaceId.name]
        )
      })
    } catch (err) {
      console.error(err)
    }
  }

  private async findDoc (_class: Ref<Class<Doc>>, _id: Ref<Doc>, forUpdate: boolean = false): Promise<Doc | undefined> {
    let query = `SELECT * FROM ${translateDomain(this.hierarchy.getDomain(_class))} WHERE _id = $1 AND "workspaceId" = $2`
    if (forUpdate) {
      query += ' FOR UPDATE'
    }
    const res = await this.client.query(query, [_id, this.workspaceId.name])
    const dbDoc = res.rows[0]
    return dbDoc !== undefined ? parseDoc(dbDoc) : undefined
  }

  protected async txRemoveDoc (tx: TxRemoveDoc<Doc>): Promise<TxResult> {
    const domain = translateDomain(this.hierarchy.getDomain(tx.objectClass))
    const res = await this.retryTxn(async (client) => {
      await client.query(`DELETE FROM ${domain} WHERE _id = $1 AND "workspaceId" = $2`, [
        tx.objectId,
        this.workspaceId.name
      ])
    })
    return res
  }
}

class PostgresTxAdapter extends PostgresAdapterBase implements TxAdapter {
  async init (domains?: string[], excludeDomains?: string[]): Promise<void> {
    const resultDomains = domains ?? [DOMAIN_TX]
    await createTable(this.client, resultDomains)
    this._helper.domains = new Set(resultDomains as Domain[])
  }

  override async tx (ctx: MeasureContext, ...tx: Tx[]): Promise<TxResult[]> {
    if (tx.length === 0) {
      return []
    }
    try {
      await this.upload(ctx, DOMAIN_TX, tx)
    } catch (err) {
      console.error(err)
    }
    return []
  }

  async getModel (ctx: MeasureContext): Promise<Tx[]> {
    const res = await this.client.query(
      `SELECT * FROM ${translateDomain(DOMAIN_TX)} WHERE "workspaceId" = '${this.workspaceId.name}'  AND data->>'objectSpace' = '${core.space.Model}' ORDER BY _id ASC, "modifiedOn" ASC`
    )
    const model = res.rows.map((p) => parseDoc<Tx>(p))
    // We need to put all core.account.System transactions first
    const systemTx: Tx[] = []
    const userTx: Tx[] = []

    model.forEach((tx) => (tx.modifiedBy === core.account.System && !isPersonAccount(tx) ? systemTx : userTx).push(tx))
    return systemTx.concat(userTx)
  }
}
/**
 * @public
 */
export async function createPostgresAdapter (
  ctx: MeasureContext,
  hierarchy: Hierarchy,
  url: string,
  workspaceId: WorkspaceId,
  modelDb: ModelDb,
  storage?: StorageAdapter
): Promise<DbAdapter> {
  const client = getDBClient(url)
  const adapter = new PostgresAdapter(await client.getClient(), client, workspaceId, hierarchy, modelDb)
  return adapter
}

/**
 * @public
 */
export async function createPostgresTxAdapter (
  ctx: MeasureContext,
  hierarchy: Hierarchy,
  url: string,
  workspaceId: WorkspaceId,
  modelDb: ModelDb
): Promise<TxAdapter> {
  const client = getDBClient(url)
  const adapter = new PostgresTxAdapter(await client.getClient(), client, workspaceId, hierarchy, modelDb)
  await adapter.init()
  return adapter
}

function isPersonAccount (tx: Tx): boolean {
  return (
    (tx._class === core.class.TxCreateDoc ||
      tx._class === core.class.TxUpdateDoc ||
      tx._class === core.class.TxRemoveDoc) &&
    ((tx as TxCUD<Doc>).objectClass === 'contact:class:PersonAccount' ||
      (tx as TxCUD<Doc>).objectClass === 'contact:class:EmployeeAccount')
  )
}
