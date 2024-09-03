//
// Copyright © 2022 Hardcore Engineering Inc.
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

import {
  type Class,
  type Doc,
  type DocumentQuery,
  type Domain,
  type FindOptions,
  type FindResult,
  type LoadModelResponse,
  type MeasureContext,
  type Ref,
  type SearchOptions,
  type SearchQuery,
  type SearchResult,
  type SessionData,
  type Timestamp,
  toFindResult,
  type Tx
} from '@hcengineering/core'
import type { Middleware, PipelineContext, TxMiddlewareResult } from './types'

/**
 * @public
 */
export abstract class BaseMiddleware implements Middleware {
  protected constructor (
    readonly context: PipelineContext,
    protected readonly next?: Middleware
  ) {}

  async findAll<T extends Doc>(
    ctx: MeasureContext<SessionData>,
    _class: Ref<Class<T>>,
    query: DocumentQuery<T>,
    options?: FindOptions<T>
  ): Promise<FindResult<T>> {
    return await this.provideFindAll(ctx, _class, query, options)
  }

  async loadModel (
    ctx: MeasureContext<SessionData>,
    lastModelTx: Timestamp,
    hash?: string
  ): Promise<Tx[] | LoadModelResponse> {
    return (await this.next?.loadModel(ctx, lastModelTx, hash)) ?? []
  }

  async provideGroupBy<T>(ctx: MeasureContext<SessionData>, domain: Domain, field: string): Promise<Set<T>> {
    if (this.next !== undefined) {
      return await this.next.groupBy(ctx, domain, field)
    }
    return new Set<T>()
  }

  async provideClose (): Promise<void> {
    if (this.next !== undefined) {
      await this.next.close()
    }
  }

  async close (): Promise<void> {
    await this.provideClose()
  }

  async groupBy<T>(ctx: MeasureContext<SessionData>, domain: Domain, field: string): Promise<Set<T>> {
    return await this.provideGroupBy(ctx, domain, field)
  }

  async searchFulltext (
    ctx: MeasureContext<SessionData>,
    query: SearchQuery,
    options: SearchOptions
  ): Promise<SearchResult> {
    return await this.provideSearchFulltext(ctx, query, options)
  }

  async handleBroadcast (
    ctx: MeasureContext<SessionData>,
    tx: Tx[],
    targets?: string | string[],
    exclude?: string[]
  ): Promise<void> {
    if (this.next !== undefined) {
      await this.next.handleBroadcast(ctx, tx, targets, exclude)
    }
  }

  protected async provideTx (ctx: MeasureContext<SessionData>, tx: Tx[]): Promise<TxMiddlewareResult> {
    if (this.next !== undefined) {
      return await this.next.tx(ctx, tx)
    }
    return {}
  }

  async tx (ctx: MeasureContext, tx: Tx[]): Promise<TxMiddlewareResult> {
    return await this.provideTx(ctx, tx)
  }

  protected async provideFindAll<T extends Doc>(
    ctx: MeasureContext,
    _class: Ref<Class<T>>,
    query: DocumentQuery<T>,
    options?: FindOptions<T>
  ): Promise<FindResult<T>> {
    if (this.next !== undefined) {
      return await this.next.findAll(ctx, _class, query, options)
    }
    return toFindResult([])
  }

  protected async provideSearchFulltext (
    ctx: MeasureContext,
    query: SearchQuery,
    options: SearchOptions
  ): Promise<SearchResult> {
    if (this.next !== undefined) {
      return await this.next.searchFulltext(ctx, query, options)
    }
    return { docs: [], total: 0 }
  }
}
