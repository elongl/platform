//
// Copyright © 2020, 2021 Anticrm Platform Contributors.
// Copyright © 2021 Hardcore Engineering Inc.
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
  TxFactory,
  TxProcessor,
  cutObjectArray,
  groupByArray,
  matchQuery,
  type Class,
  type Doc,
  type DocumentQuery,
  type Hierarchy,
  type MeasureContext,
  type ModelDb,
  type Obj,
  type Ref,
  type Tx,
  type TxCreateDoc,
  type WithLookup
} from '@hcengineering/core'

import { Analytics } from '@hcengineering/analytics'
import { getResource, type Resource } from '@hcengineering/platform'
import type { Trigger, TriggerControl, TriggerFunc } from './types'

import serverCore from './plugin'
import type { SessionDataImpl } from './utils'

interface TriggerRecord {
  query?: DocumentQuery<Tx>
  trigger: { op: TriggerFunc | Promise<TriggerFunc>, resource: Resource<TriggerFunc>, isAsync: boolean }

  arrays: boolean
}
/**
 * @public
 */
export class Triggers {
  private readonly triggers: TriggerRecord[] = []

  constructor (protected readonly hierarchy: Hierarchy) {}

  init (model: ModelDb): void {
    const allTriggers = model.findAllSync(serverCore.class.Trigger, {})
    for (const t of allTriggers) {
      this.addTrigger(t)
    }
  }

  private addTrigger (t: WithLookup<Trigger>): void {
    const match = t.txMatch

    const trigger = t.trigger
    const func = getResource(trigger)
    const isAsync = t.isAsync === true
    this.triggers.push({
      query: match,
      trigger: { op: func, resource: trigger, isAsync },
      arrays: t.arrays === true
    })
  }

  async tx (txes: Tx[]): Promise<void> {
    for (const tx of txes) {
      if (tx._class === core.class.TxCreateDoc) {
        const createTx = tx as TxCreateDoc<Doc>
        if (createTx.objectClass === serverCore.class.Trigger) {
          const trigger = TxProcessor.createDoc2Doc(createTx as TxCreateDoc<Trigger>)
          this.addTrigger(trigger)
        }
      }
    }
  }

  async apply (
    ctx: MeasureContext,
    tx: Tx[],
    ctrl: Omit<TriggerControl, 'txFactory'>
  ): Promise<{
      transactions: Tx[]
      performAsync?: (ctx: MeasureContext) => Promise<Tx[]>
    }> {
    const result: Tx[] = []
    const apply: Tx[] = []

    const suppressAsync = (ctx as MeasureContext<SessionDataImpl>).contextData.isAsyncContext ?? false

    const asyncRequest: {
      matches: Tx[]
      trigger: TriggerRecord['trigger']
      arrays: TriggerRecord['arrays']
    }[] = []

    const applyTrigger = async (
      ctx: MeasureContext,
      matches: Tx[],
      { trigger, arrays }: TriggerRecord,
      result: Tx[],
      apply: Tx[]
    ): Promise<void> => {
      const group = groupByArray(matches, (it) => it.modifiedBy)

      const tctrl: TriggerControl = {
        ...ctrl,
        ctx,
        txFactory: null as any, // Will be set later
        apply: async (ctx, tx, needResult) => {
          apply.push(...tx)
          return await ctrl.apply(ctx, tx, needResult)
        },
        txes: {
          apply,
          result
        }
      }
      if (trigger.op instanceof Promise) {
        trigger.op = await trigger.op
      }
      for (const [k, v] of group.entries()) {
        const m = arrays ? [v] : v
        tctrl.txFactory = new TxFactory(k, true)
        for (const tx of m) {
          try {
            result.push(...(await trigger.op(tx, tctrl)))
          } catch (err: any) {
            ctx.error('failed to process trigger', { trigger: trigger.resource, tx, err })
            Analytics.handleError(err)
          }
        }
      }
    }

    for (const { query, trigger, arrays } of this.triggers) {
      let matches = tx
      if (query !== undefined) {
        this.addDerived(query, 'objectClass')
        this.addDerived(query, 'tx.objectClass')
        matches = matchQuery(tx, query, core.class.Tx, ctrl.hierarchy) as Tx[]
      }
      if (matches.length > 0) {
        if (trigger.isAsync && !suppressAsync) {
          asyncRequest.push({
            matches,
            trigger,
            arrays
          })
        } else {
          await ctx.with(
            trigger.resource,
            {},
            async (ctx) => {
              const tresult: Tx[] = []
              const tapply: Tx[] = []
              await applyTrigger(ctx, matches, { trigger, arrays }, tresult, tapply)
              result.push(...tresult)
              apply.push(...tapply)
            },
            { count: matches.length, arrays }
          )
        }
      }
    }
    return {
      transactions: result,
      performAsync:
        asyncRequest.length > 0
          ? async (ctx) => {
            // If we have async triggers let's sheculed them after IO phase.
            const result: Tx[] = []
            const apply: Tx[] = []
            for (const request of asyncRequest) {
              try {
                await ctx.with(request.trigger.resource, {}, async (ctx) => {
                  await applyTrigger(ctx, request.matches, request, result, apply)
                })
              } catch (err: any) {
                ctx.error('failed to process trigger', {
                  trigger: request.trigger.resource,
                  matches: cutObjectArray(request.matches),
                  err
                })
                Analytics.handleError(err)
              }
            }
            return result
          }
          : undefined
    }
  }

  private addDerived (q: DocumentQuery<Tx>, key: string): void {
    if (q[key] === undefined) {
      return
    }
    if (typeof q[key] === 'string') {
      const descendants = this.hierarchy.getDescendants(q[key] as Ref<Class<Doc>>)
      q[key] = {
        $in: [q[key] as Ref<Class<Doc>>, ...descendants]
      }
    } else {
      if (Array.isArray(q[key].$in)) {
        const oldIn = q[key].$in
        const newIn = new Set(oldIn)
        q[key].$in.forEach((element: Ref<Class<Obj>>) => {
          const descendants = this.hierarchy.getDescendants(element)
          descendants.forEach((d) => newIn.add(d))
        })
        q[key].$in = Array.from(newIn.values())
      }
      if (Array.isArray(q[key].$nin)) {
        const oldNin = q[key].$nin
        const newNin = new Set(oldNin)
        q[key].$nin.forEach((element: Ref<Class<Obj>>) => {
          const descendants = this.hierarchy.getDescendants(element)
          descendants.forEach((d) => newNin.add(d))
        })
        q[key].$nin = Array.from(newNin.values())
      }
      if (q[key].$ne !== undefined) {
        const descendants = this.hierarchy.getDescendants(q[key].$ne)
        delete q[key].$ne
        q[key].$nin = [...(q[key].$nin ?? []), ...descendants]
      }
    }
  }
}
