//
// Copyright © 2023 Hardcore Engineering Inc.
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

import activity, {
  ActivityMessage,
  ActivityMessageControl,
  DocAttributeUpdates,
  DocUpdateMessage,
  Reaction
} from '@hcengineering/activity'
import { PersonAccount } from '@hcengineering/contact'
import core, {
  Account,
  AttachedDoc,
  Class,
  Collection,
  Data,
  Doc,
  Hierarchy,
  matchQuery,
  MeasureContext,
  Ref,
  Space,
  Tx,
  TxCollectionCUD,
  TxCreateDoc,
  TxCUD,
  TxProcessor
} from '@hcengineering/core'
import notification, { NotificationContent } from '@hcengineering/notification'
import { getResource, translate } from '@hcengineering/platform'
import { ActivityControl, DocObjectCache } from '@hcengineering/server-activity'
import type { TriggerControl } from '@hcengineering/server-core'
import {
  createCollabDocInfo,
  createCollaboratorNotifications,
  getTextPresenter,
  removeDocInboxNotifications
} from '@hcengineering/server-notification-resources'

import { ReferenceTrigger } from './references'
import { getAttrName, getCollectionAttribute, getDocUpdateAction, getTxAttributesUpdates } from './utils'

export async function OnReactionChanged (originTx: Tx, control: TriggerControl): Promise<Tx[]> {
  const tx = originTx as TxCollectionCUD<ActivityMessage, Reaction>
  const innerTx = TxProcessor.extractTx(tx) as TxCUD<Reaction>

  if (innerTx._class === core.class.TxCreateDoc) {
    const txes = await createReactionNotifications(tx, control)

    await control.apply(control.ctx, txes, true)
    return []
  }

  if (innerTx._class === core.class.TxRemoveDoc) {
    const txes = await removeReactionNotifications(tx, control)
    await control.apply(control.ctx, txes, true)
    return []
  }

  return []
}

export async function removeReactionNotifications (
  tx: TxCollectionCUD<ActivityMessage, Reaction>,
  control: TriggerControl
): Promise<Tx[]> {
  const message = (
    await control.findAll(
      control.ctx,
      activity.class.ActivityMessage,
      { objectId: tx.tx.objectId },
      { projection: { _id: 1, _class: 1, space: 1 } }
    )
  )[0]

  if (message === undefined) {
    return []
  }

  const res: Tx[] = []
  const txes = await removeDocInboxNotifications(message._id, control)

  const removeTx = control.txFactory.createTxRemoveDoc(message._class, message.space, message._id)

  res.push(removeTx)
  res.push(...txes)

  return res
}

export async function createReactionNotifications (
  tx: TxCollectionCUD<ActivityMessage, Reaction>,
  control: TriggerControl
): Promise<Tx[]> {
  const createTx = TxProcessor.extractTx(tx) as TxCreateDoc<Reaction>

  const parentMessage = (await control.findAll(control.ctx, activity.class.ActivityMessage, { _id: tx.objectId }))[0]

  if (parentMessage === undefined) {
    return []
  }

  const user = parentMessage.createdBy

  if (user === undefined || user === core.account.System || user === tx.modifiedBy) {
    return []
  }

  let res: Tx[] = []

  const rawMessage: Data<DocUpdateMessage> = {
    txId: tx._id,
    attachedTo: parentMessage._id,
    attachedToClass: parentMessage._class,
    objectId: createTx.objectId,
    objectClass: createTx.objectClass,
    action: 'create',
    collection: 'docUpdateMessages',
    updateCollection: tx.collection
  }

  const messageTx = getDocUpdateMessageTx(control, tx, parentMessage, rawMessage, tx.modifiedBy)

  if (messageTx === undefined) {
    return []
  }

  res.push(messageTx)

  const docUpdateMessage = TxProcessor.createDoc2Doc(messageTx.tx as TxCreateDoc<DocUpdateMessage>)

  res = res.concat(
    await createCollabDocInfo(
      control.ctx,
      [user] as Ref<PersonAccount>[],
      control,
      tx.tx,
      tx,
      parentMessage,
      [docUpdateMessage],
      { isOwn: true, isSpace: false, shouldUpdateTimestamp: false }
    )
  )

  return res
}

function isActivityDoc (_class: Ref<Class<Doc>>, hierarchy: Hierarchy): boolean {
  const mixin = hierarchy.classHierarchyMixin(_class, activity.mixin.ActivityDoc)

  return mixin !== undefined
}

function isSpace (space: Doc, hierarchy: Hierarchy): space is Space {
  return hierarchy.isDerived(space._class, core.class.Space)
}

function getDocUpdateMessageTx (
  control: ActivityControl,
  originTx: TxCUD<Doc>,
  object: Doc,
  rawMessage: Data<DocUpdateMessage>,
  modifiedBy?: Ref<Account>
): TxCollectionCUD<Doc, DocUpdateMessage> {
  const { hierarchy } = control
  const space = isSpace(object, hierarchy) ? object._id : object.space
  const innerTx = control.txFactory.createTxCreateDoc(
    activity.class.DocUpdateMessage,
    space,
    rawMessage,
    undefined,
    originTx.modifiedOn,
    modifiedBy ?? originTx.modifiedBy
  )

  return control.txFactory.createTxCollectionCUD(
    rawMessage.attachedToClass,
    rawMessage.attachedTo,
    space,
    rawMessage.collection,
    innerTx,
    originTx.modifiedOn,
    modifiedBy ?? originTx.modifiedBy
  )
}

export async function pushDocUpdateMessages (
  ctx: MeasureContext,
  control: ActivityControl,
  res: TxCollectionCUD<Doc, DocUpdateMessage>[],
  object: Doc | undefined,
  originTx: TxCUD<Doc>,
  modifiedBy?: Ref<Account>,
  objectCache?: DocObjectCache,
  controlRules?: ActivityMessageControl[]
): Promise<TxCollectionCUD<Doc, DocUpdateMessage>[]> {
  if (object === undefined) {
    return res
  }

  if (!isActivityDoc(object._class, control.hierarchy)) {
    return res
  }

  const tx =
    originTx._class === core.class.TxCollectionCUD ? (originTx as TxCollectionCUD<Doc, AttachedDoc>).tx : originTx

  const rawMessage: Data<DocUpdateMessage> = {
    txId: originTx._id,
    attachedTo: object._id,
    attachedToClass: object._class,
    objectId: tx.objectId,
    objectClass: tx.objectClass,
    action: getDocUpdateAction(control, tx),
    collection: 'docUpdateMessages',
    updateCollection:
      originTx._class === core.class.TxCollectionCUD
        ? (originTx as TxCollectionCUD<Doc, AttachedDoc>).collection
        : undefined
  }

  const attributesUpdates = await getTxAttributesUpdates(ctx, control, originTx, tx, object, objectCache, controlRules)

  for (const attributeUpdates of attributesUpdates) {
    res.push(
      getDocUpdateMessageTx(
        control,
        originTx,
        object,
        {
          ...rawMessage,
          attributeUpdates
        },
        modifiedBy
      )
    )
  }

  if (attributesUpdates.length === 0 && rawMessage.action !== 'update') {
    res.push(getDocUpdateMessageTx(control, originTx, object, rawMessage, modifiedBy))
  }

  return res
}

export async function generateDocUpdateMessages (
  ctx: MeasureContext,
  tx: TxCUD<Doc>,
  control: ActivityControl,
  res: TxCollectionCUD<Doc, DocUpdateMessage>[] = [],
  originTx?: TxCUD<Doc>,
  objectCache?: DocObjectCache
): Promise<TxCollectionCUD<Doc, DocUpdateMessage>[]> {
  if (tx.space === core.space.DerivedTx) {
    return res
  }

  const { hierarchy } = control
  const etx = TxProcessor.extractTx(tx) as TxCUD<Doc>

  if (
    hierarchy.isDerived(tx.objectClass, activity.class.ActivityMessage) ||
    hierarchy.isDerived(etx.objectClass, activity.class.ActivityMessage)
  ) {
    return res
  }

  if (
    hierarchy.classHierarchyMixin(tx.objectClass, activity.mixin.IgnoreActivity) !== undefined ||
    hierarchy.classHierarchyMixin(etx.objectClass, activity.mixin.IgnoreActivity) !== undefined
  ) {
    return res
  }

  // Check if we have override control over transaction => activity mappings
  const controlRules = control.modelDb.findAllSync<ActivityMessageControl>(activity.class.ActivityMessageControl, {
    objectClass: { $in: hierarchy.getAncestors(tx.objectClass) }
  })
  if (controlRules.length > 0) {
    for (const r of controlRules) {
      for (const s of r.skip) {
        const otx = originTx ?? etx
        if (matchQuery(otx !== undefined ? [tx, otx] : [tx], s, r.objectClass, hierarchy).length > 0) {
          // Match found, we need to skip
          return res
        }
      }
    }
  }

  switch (tx._class) {
    case core.class.TxCreateDoc: {
      const doc = TxProcessor.createDoc2Doc(tx as TxCreateDoc<Doc>)
      return await ctx.with(
        'pushDocUpdateMessages',
        {},
        async (ctx) =>
          await pushDocUpdateMessages(ctx, control, res, doc, originTx ?? tx, undefined, objectCache, controlRules)
      )
    }
    case core.class.TxMixin:
    case core.class.TxUpdateDoc: {
      if (!isActivityDoc(tx.objectClass, control.hierarchy)) {
        return res
      }

      let doc = objectCache?.docs?.get(tx.objectId)
      if (doc === undefined) {
        doc = (await control.findAll(ctx, tx.objectClass, { _id: tx.objectId }, { limit: 1 }))[0]
        objectCache?.docs?.set(tx.objectId, doc)
      }
      return await ctx.with(
        'pushDocUpdateMessages',
        {},
        async (ctx) =>
          await pushDocUpdateMessages(
            ctx,
            control,
            res,
            doc ?? undefined,
            originTx ?? tx,
            undefined,
            objectCache,
            controlRules
          )
      )
    }
    case core.class.TxCollectionCUD: {
      const actualTx = TxProcessor.extractTx(tx) as TxCUD<Doc>
      res = await generateDocUpdateMessages(ctx, actualTx, control, res, tx, objectCache)
      if ([core.class.TxCreateDoc, core.class.TxRemoveDoc].includes(actualTx._class)) {
        if (!isActivityDoc(tx.objectClass, control.hierarchy)) {
          return res
        }

        let doc = objectCache?.docs?.get(tx.objectId)
        if (doc === undefined) {
          doc = (await control.findAll(ctx, tx.objectClass, { _id: tx.objectId }, { limit: 1 }))[0]
          objectCache?.docs?.set(tx.objectId, doc)
        }
        if (doc !== undefined) {
          return await ctx.with(
            'pushDocUpdateMessages',
            {},
            async (ctx) =>
              await pushDocUpdateMessages(
                ctx,
                control,
                res,
                doc ?? undefined,
                originTx ?? tx,
                undefined,
                objectCache,
                controlRules
              )
          )
        }
      }
      return res
    }
  }

  return res
}

async function ActivityMessagesHandler (tx: TxCUD<Doc>, control: TriggerControl): Promise<Tx[]> {
  if (
    control.hierarchy.isDerived(tx.objectClass, activity.class.ActivityMessage) ||
    control.hierarchy.isDerived(tx.objectClass, notification.class.DocNotifyContext) ||
    control.hierarchy.isDerived(tx.objectClass, notification.class.ActivityInboxNotification) ||
    control.hierarchy.isDerived(tx.objectClass, notification.class.BrowserNotification)
  ) {
    return []
  }

  const cache: DocObjectCache = control.contextCache.get('ActivityMessagesHandler') ?? {
    docs: new Map(),
    transactions: new Map()
  }
  control.contextCache.set('ActivityMessagesHandler', cache)

  const txes =
    tx.space === core.space.DerivedTx
      ? []
      : await control.ctx.with(
        'generateDocUpdateMessages',
        {},
        async (ctx) => await generateDocUpdateMessages(ctx, tx, control, [], undefined, cache)
      )

  const messages = txes.map((messageTx) => TxProcessor.createDoc2Doc(messageTx.tx as TxCreateDoc<DocUpdateMessage>))

  const notificationTxes = await control.ctx.with(
    'createCollaboratorNotifications',
    {},
    async (ctx) =>
      await createCollaboratorNotifications(ctx, tx, control, messages, undefined, cache.docs as Map<Ref<Doc>, Doc>)
  )

  const result = [...txes, ...notificationTxes]

  if (result.length > 0) {
    await control.apply(control.ctx, result)
  }
  return []
}

async function OnDocRemoved (originTx: TxCUD<Doc>, control: TriggerControl): Promise<Tx[]> {
  const tx = TxProcessor.extractTx(originTx) as TxCUD<Doc>

  if (tx._class !== core.class.TxRemoveDoc) {
    return []
  }

  const activityDocMixin = control.hierarchy.classHierarchyMixin(tx.objectClass, activity.mixin.ActivityDoc)

  if (activityDocMixin === undefined) {
    return []
  }

  const messages = await control.findAll(
    control.ctx,
    activity.class.ActivityMessage,
    { attachedTo: tx.objectId },
    { projection: { _id: 1, _class: 1, space: 1 } }
  )

  return messages.map((message) => control.txFactory.createTxRemoveDoc(message._class, message.space, message._id))
}

async function ReactionNotificationContentProvider (
  doc: ActivityMessage,
  originTx: TxCUD<Doc>,
  _: Ref<Account>,
  control: TriggerControl
): Promise<NotificationContent> {
  const tx = TxProcessor.extractTx(originTx) as TxCreateDoc<Reaction>
  const presenter = getTextPresenter(doc._class, control.hierarchy)
  const reaction = TxProcessor.createDoc2Doc(tx)

  let text = ''

  if (presenter !== undefined) {
    const fn = await getResource(presenter.presenter)

    text = await fn(doc, control)
  } else {
    text = await translate(activity.string.Message, {})
  }

  return {
    title: activity.string.ReactionNotificationTitle,
    body: activity.string.ReactionNotificationBody,
    data: reaction.emoji,
    intlParams: {
      title: text,
      reaction: reaction.emoji
    }
  }
}

async function getAttributesUpdatesText (
  attributeUpdates: DocAttributeUpdates,
  objectClass: Ref<Class<Doc>>,
  hierarchy: Hierarchy
): Promise<string | undefined> {
  const attrName = await getAttrName(attributeUpdates, objectClass, hierarchy)

  if (attrName === undefined) {
    return undefined
  }

  if (attributeUpdates.added.length > 0) {
    return await translate(activity.string.NewObject, { object: attrName })
  }
  if (attributeUpdates.removed.length > 0) {
    return await translate(activity.string.RemovedObject, { object: attrName })
  }

  if (attributeUpdates.set.length > 0) {
    const values = attributeUpdates.set
    const isUnset = values.length > 0 && !values.some((value) => value !== null && value !== '')

    if (isUnset) {
      return await translate(activity.string.UnsetObject, { object: attrName })
    } else {
      return await translate(activity.string.ChangedObject, { object: attrName })
    }
  }

  return undefined
}

export async function DocUpdateMessageTextPresenter (doc: DocUpdateMessage, control: TriggerControl): Promise<string> {
  const { hierarchy } = control
  const { attachedTo, attachedToClass, objectClass, objectId, action, updateCollection, attributeUpdates } = doc
  const isOwn = attachedTo === objectId

  const collectionAttribute = getCollectionAttribute(hierarchy, attachedToClass, updateCollection)
  const clazz = hierarchy.getClass(objectClass)
  const objectName = (collectionAttribute?.type as Collection<AttachedDoc>)?.itemLabel ?? clazz.label
  const collectionName = collectionAttribute?.label

  const name =
    isOwn || collectionName === undefined ? await translate(objectName, {}) : await translate(collectionName, {})

  if (action === 'create') {
    return await translate(activity.string.NewObject, { object: name })
  }

  if (action === 'remove') {
    return await translate(activity.string.RemovedObject, { object: name })
  }

  if (action === 'update' && attributeUpdates !== undefined) {
    const text = await getAttributesUpdatesText(attributeUpdates, objectClass, hierarchy)

    if (text !== undefined) {
      return text
    }
  }

  return await translate(activity.string.UpdatedObject, { object: name })
}

export * from './references'

// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
export default async () => ({
  trigger: {
    ReferenceTrigger,
    ActivityMessagesHandler,
    OnDocRemoved,
    OnReactionChanged
  },
  function: {
    ReactionNotificationContentProvider,
    DocUpdateMessageTextPresenter
  }
})
