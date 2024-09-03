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

import {
  type Class,
  type Doc,
  type DocumentQuery,
  type Domain,
  type FindResult,
  type MeasureContext,
  type Ref,
  DOMAIN_MODEL,
  toFindResult
} from '@hcengineering/core'
import { PlatformError, unknownError } from '@hcengineering/platform'
import type { DBAdapterManager, Middleware, PipelineContext, ServerFindOptions } from '@hcengineering/server-core'
import { BaseMiddleware } from '@hcengineering/server-core'

/**
 * Will perform a find inside adapters
 * @public
 */
export class DomainFindMiddleware extends BaseMiddleware implements Middleware {
  adapterManager!: DBAdapterManager

  static async create (ctx: MeasureContext, context: PipelineContext, next?: Middleware): Promise<Middleware> {
    const middleware = new DomainFindMiddleware(context, next)
    if (context.adapterManager == null) {
      throw new PlatformError(unknownError('Adapter maneger should be configured'))
    }
    middleware.adapterManager = context.adapterManager
    return middleware
  }

  async findAll<T extends Doc>(
    ctx: MeasureContext,
    _class: Ref<Class<T>>,
    query: DocumentQuery<T>,
    options?: ServerFindOptions<T>
  ): Promise<FindResult<T>> {
    if (query?.$search !== undefined) {
      // Server storage pass $search queries to next
      return (await this.next?.findAll(ctx, _class, query, options)) ?? toFindResult([])
    }
    const p = options?.prefix ?? 'client'
    const domain = options?.domain ?? this.context.hierarchy.getDomain(_class)
    if (domain === DOMAIN_MODEL) {
      return this.context.modelDb.findAllSync(_class, query, options)
    }
    const result = await ctx.with(
      p + '-find-all',
      { _class },
      (ctx) => {
        return this.adapterManager.getAdapter(domain, false).findAll(ctx, _class, query, options)
      },
      { _class, query, options }
    )
    return result
  }

  async groupBy<T>(ctx: MeasureContext, domain: Domain, field: string): Promise<Set<T>> {
    return await this.adapterManager.getAdapter(domain, false).groupBy(ctx, domain, field)
  }
}
