import { Db, MongoClient, ObjectId } from 'mongodb'
import { deepmerge } from 'deepmerge-ts'
import fs from 'fs/promises'
import { randomUUID } from 'node:crypto'
import {
  sampleRev,
  sampleSlide1,
  sampleSlide2,
  sampleSlideUpdate1,
  sampleTrack1,
  sampleTrack2,
  sampleTrack3,
} from './sample'

type Obj = {
  id: string
}

interface User {
  email: string
}

interface Entity<T extends Obj> {
  entity: string
  event: 'created' | 'updated' | 'deleted'
  logTime: Date
  object: T
  blameUser: User
}

interface TrackObj extends Obj {
  type: string
  seconds: number | null
  slideId: string
  questions: string[]
}

interface SlideObj extends Obj {
  position: number
  type: 'media' | 'elearn'
  mediaUrl: string | null
  revisionId: string
  createdAt: Date
}

interface RevObj extends Obj {
  status: string
  major: number
  minor: number
  patch: number
  id: string
  createdAt: Date
}

interface TrackSchema extends Entity<TrackObj> {
  _id: ObjectId
  _createdAt: Date
}

interface SlideSchema extends Entity<SlideObj> {
  _id: ObjectId
  _createdAt: Date
  tracks: TrackSchema[]
  previous: null | SlideSchema
}

interface RevisionSchema extends Entity<RevObj> {
  _id: ObjectId
  _createdAt: Date
  previous: null | RevisionSchema
}

async function insertRev(db: Db, rev: Entity<RevObj>) {
  const inserted = await db.collection('revAudits').insertOne({
    ...rev,
    _createdAt: new Date(),
  })
  return insertDenormalized(db, {
    ...rev,
    _id: inserted.insertedId,
  })
}

async function insertSlide(db: Db, { ...slide }: Entity<SlideObj>) {
  const revAudits = await db.collection('revAudits')
  const slideAudits = await db.collection('slideAudits')
  const rev = await revAudits.findOne(
    {
      'object.id': slide.object.revisionId,
    },
    {
      sort: {
        _createdAt: -1,
      },
    },
  )

  const newSlide = {
    ...slide,
    _createdAt: new Date(),
  }

  const inserted = await slideAudits.insertOne(newSlide)

  await insertDenormalizedSlide(db, {
    ...newSlide,
    _id: inserted.insertedId,
  })

  await insertDenormalized(db, rev! as any, {
    type: 'slide',
    item: {
      ...newSlide,
      _id: inserted.insertedId,
    } as Entity<SlideObj>,
  })
}

async function insertTrack(db: Db, track: Entity<TrackObj>) {
  const revAudits = await db.collection('revAudits')
  const slideAudits = await db.collection('slideAudits')
  const trackAudits = await db.collection('trackAudits')

  const slide = await slideAudits.findOne(
    {
      'object.id': track.object.slideId,
    },
    {
      sort: {
        _createdAt: -1,
      },
    },
  )

  const rev = await revAudits.findOne({
    'object.id': slide?.object.revisionId,
  })

  const newTrack = {
    ...track,
    _createdAt: new Date(),
  }
  const trackId = await trackAudits.insertOne(newTrack)

  await insertDenormalizedSlide(
    db,
    slide! as any,
    {
      _id: trackId.insertedId,
      ...newTrack,
    } as Entity<TrackObj>,
  )

  await insertDenormalized(db, rev! as any, {
    type: 'track',
    item: {
      _id: trackId.insertedId,
      ...newTrack,
    } as Entity<TrackObj>,
  })
}

async function insertDenormalizedSlide(
  db: Db,
  item: Entity<SlideObj> | SlideSchema,
  children?: Entity<TrackObj>,
) {
  const col = db.collection<SlideSchema>('denormalizedSlideAudits')

  const prev = await col.findOne(
    {
      'object.id': item.object.id,
    },
    {
      sort: {
        _createdAt: -1,
      },
    },
  )

  const newId = new ObjectId()
  let nextSlide
  if (prev === null) {
    nextSlide = {
      ...item,
      previous: null,
      tracks: [] as TrackSchema[],
      _id: newId,
      _createdAt: new Date(),
    }
  } else {
    const { _id: prevId, previous, ...prevBody } = prev
    nextSlide = {
      /// use structured clone for deep copy
      /// as prevBody could be changed by nextSlide
      ...prevBody,
      ...item,
      previous: prevId,
      event: 'updated',
      _id: newId,
      _createdAt: new Date(),
    }
  }

  if (children) {
    if (children.event === 'created') {
      nextSlide.tracks = [...(nextSlide.tracks || []), children as TrackSchema]
    } else if (children.event === 'updated') {
      nextSlide.tracks = (nextSlide.tracks || []).map((s) => {
        if (s.object.id === children.object.id) {
          return deepmerge(s, children) as TrackSchema
        }
        return s as TrackSchema
      })
    } else if (children.event === 'deleted') {
      nextSlide.tracks = (nextSlide.tracks || []).filter((s: TrackSchema) => {
        return s.object.id !== children.object.id
      })
    } else {
      // noop
    }
  }

  return col.insertOne(nextSlide as SlideSchema)
}
async function insertDenormalized(
  db: Db,
  item:
    | Entity<RevObj>
    | (RevisionSchema & {
        slides: SlideSchema[]
      }),
  children?: {
    type: 'slide' | 'track'
    item: Entity<SlideObj> | Entity<TrackObj>
  },
) {
  const col = db.collection<
    RevisionSchema & {
      slides: SlideSchema[]
    }
  >('denormalizedRevAudits')
  const prev = await col.findOne(
    {
      'object.id': item.object.id,
    },
    {
      sort: {
        _createdAt: -1,
      },
    },
  )

  const newId = new ObjectId()
  let nextRev
  if (prev === null) {
    nextRev = {
      ...item,
      previous: null,
      slides: [],
      _id: newId,
      _createdAt: new Date(),
    }
  } else {
    const { _id: prevId, previous, ...prevBody } = prev
    nextRev = {
      /// use structured clone for deep copy
      /// as prevBody could be change by nextRev
      ...prevBody,
      ...item,
      previous: prevId,
      event: 'updated',
      _id: newId,
      _createdAt: new Date(),
    }
  }

  if (children) {
    const { type, item: _item } = children
    if (type === 'slide') {
      const item = _item as Entity<SlideObj>
      if (item?.event === 'created') {
        nextRev.slides = [...nextRev.slides, item as SlideSchema]
      } else if (item?.event === 'updated') {
        nextRev.slides = (nextRev.slides || []).map((s) => {
          if (s.object.id === item.object.id) {
            return deepmerge(s, item) as SlideSchema
          }
          return s as SlideSchema
        })
      } else if (item?.event === 'deleted') {
        nextRev.slides = (nextRev.slides || []).filter((s: SlideSchema) => {
          return s.object.id !== item.object.id
        })
      } else {
        // noop
      }
    } else {
      const item = _item as Entity<TrackObj>
      if (item?.event === 'created') {
        nextRev.slides = (nextRev.slides || []).map((s: SlideSchema) => {
          if (s.object.id === item.object.slideId) {
            s.tracks = [...(s.tracks || []), item as TrackSchema]
            return s
          }
          return s
        })
      } else if (item?.event === 'updated') {
        nextRev.slides = (nextRev.slides || []).map((s: SlideSchema) => {
          if (s.object.id === item.object.slideId) {
            s.tracks = (s.tracks || []).map((t) => {
              if (t.object.id === item.object.id) {
                return deepmerge(t, item as TrackSchema)
              }
              return t
            })
            return s
          }
          return s
        })
      } else if (item?.event === 'deleted') {
        nextRev.slides = (nextRev.slides || []).map((s: SlideSchema) => {
          if (s.object.id === item.object.slideId) {
            s.tracks = (s.tracks || []).filter(
              (t) => t.object.id !== item.object.id,
            )
            return s
          }
          return s
        })
      } else {
        // noop
      }
    }
  }

  return col.insertOne(nextRev as any)
  //
}

interface Slide {
  id: string
  tracks: Track[]
}

interface Track {
  id: string
  lessonSlideId: string
  type: string
  seconds: number | null
  setting: Record<string, unknown>
  questions: Record<string, unknown>[]
}

function transformSlide(data: SlideSchema): Record<string, unknown> {
  const { _id, object, ...rest } = data
  return {
    _id,
    lessonSlide: object,
    //lessonSlide: {
    ...rest,
    //},
  }
}

function transformTrack(data: TrackSchema) {
  const { _id, object, blameUser } = data
  return object
}

function transformSlide2(
  data: SlideSchema,
  depth: number,
): Record<string, unknown> {
  const { tracks, object, previous, ...rest } = data

  const dt: Record<string, unknown> = {
    ...rest,
    lessonSlide: {
      ...object,
      tracks: tracks.map(transformTrack),
    },
  }

  if (depth === 0 && Boolean(previous)) {
    dt['previous'] = transformSlide2(previous as SlideSchema, depth + 1)
    dt['previous'] = (dt['previous'] as Record<string, Record<string, unknown>>)
      .lessonSlide as Record<string, unknown>
  }

  return dt
}

function transformRev(
  data: Record<string, unknown>,
  depth: number,
): Record<string, unknown> {
  const { slides, object, previous, ...rest } =
    (data as Record<string, unknown> & {
      _id: ObjectId
      object: RevObj
    }) || {}

  const dt: Record<string, unknown> = {
    ...rest,
    lessonRevision: {
      version: `${object.major}.${object.minor}.${object.patch}`,
      status: object.status,
      id: `${object?.id}`,
      createdAt: `${object?.createdAt}`,
      slides: ((slides || []) as SlideSchema[]).map((s) => {
        return transformSlide(s)
      }),
    },
  }

  if (depth === 0 && Boolean(previous)) {
    dt['previous'] = transformRev(
      previous as Record<string, unknown>,
      depth + 1,
    )
    dt['previous'] = (dt['previous'] as Record<string, Record<string, unknown>>)
      .lessonRevision as Record<string, unknown>
  }

  return dt
}

async function main() {
  const client = new MongoClient('mongodb://root:root@localhost:27018')
  const db = client.db('rootdb')

  await db.dropCollection('revAudits')
  await db.createCollection('revAudits')
  await db.dropCollection('slideAudits')
  await db.createCollection('slideAudits')
  await db.dropCollection('trackAudits')
  await db.createCollection('trackAudits')
  await db.dropCollection('denormalizedSlideAudits')
  const denormalizedSlideAudits = await db.createCollection(
    'denormalizedSlideAudits',
  )
  await db.dropCollection('denormalizedRevAudits')
  const denormalizedRevAudits = await db.createCollection(
    'denormalizedRevAudits',
  )

  await insertRev(db, sampleRev as Entity<RevObj>)
  await insertSlide(db, sampleSlide1 as Entity<SlideObj>)
  await insertSlide(db, sampleSlideUpdate1 as Entity<SlideObj>)
  await insertTrack(db, sampleTrack1 as Entity<TrackObj>)
  await insertTrack(db, sampleTrack2 as Entity<TrackObj>)
  await insertSlide(db, sampleSlide2 as Entity<SlideObj>)
  await insertTrack(db, sampleTrack3 as Entity<TrackObj>)
  await insertTrack(db, {
    ...sampleTrack2,
    event: 'deleted',
  } as Entity<TrackObj>)

  /*
  await insertSlide(db, {
    ...sampleSlide2,
    event: 'deleted',
  } as Entity<SlideObj>)*/

  const all = await denormalizedRevAudits
    .aggregate([
      {
        $lookup: {
          from: 'denormalizedRevAudits',
          localField: 'previous',
          foreignField: '_id',
          as: 'previous',
        },
      },
      {
        $project: {
          id: 0,
          createdAt: 0,
          logTime: 0,
          entity: 0,
          ipAddress: 0,
          userAgent: 0,
          previous: {
            id: 0,
            createdAt: 0,
            previous: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
            slides: {
              id: 0,
              createdAt: 0,
              logTime: 0,
              entity: 0,
              ipAddress: 0,
              userAgent: 0,
              object: 0,
              tracks: {
                setting: 0,
                tags: 0,
                logTime: 0,
                entity: 0,
                ipAddress: 0,
                userAgent: 0,
                object: 0,
              },
            },
          },
          slides: {
            id: 0,
            createdAt: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
            object: 0,
            tracks: {
              setting: 0,
              tags: 0,
              logTime: 0,
              entity: 0,
              ipAddress: 0,
              userAgent: 0,
              object: 0,
            },
          },
        },
      },
      /*{
        $project: {
          object: {
            id: 1,
            major: 1,
            minor: 1,
            patch: 1,
            createdAt: 1,
          },
          previous: {
            object: {
              id: 1,
              major: 1,
              minor: 1,
              patch: 1,
              createdAt: 1,
            },
          },
        },
      },*/
    ])
    .map((item) => {
      return {
        ...item,
        previous: Array.isArray(item.previous) ? item.previous[0] : null,
      }
    })
    .toArray()

  //const transformed = transformRev(all as RevisionSchema)

  const dt = await denormalizedRevAudits
    .aggregate([
      {
        $lookup: {
          from: 'denormalizedRevAudits',
          localField: 'previous',
          foreignField: '_id',
          as: 'previous',
        },
      },
      {
        $project: {
          id: 0,
          createdAt: 0,
          _createdAt: 0,
          logTime: 0,
          entity: 0,
          ipAddress: 0,
          userAgent: 0,
          previous: {
            id: 0,
            createdAt: 0,
            _createdAt: 0,
            previous: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
            slides: {
              id: 0,
              createdAt: 0,
              _createdAt: 0,
              logTime: 0,
              entity: 0,
              ipAddress: 0,
              userAgent: 0,
              tracks: {
                setting: 0,
                tags: 0,
                id: 0,
                createdAt: 0,
                _createdAt: 0,
                logTime: 0,
                entity: 0,
                ipAddress: 0,
                userAgent: 0,
              },
            },
          },
          slides: {
            id: 0,
            createdAt: 0,
            _createdAt: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
            tracks: {
              setting: 0,
              tags: 0,
              id: 0,
              createdAt: 0,
              _createdAt: 0,
              logTime: 0,
              entity: 0,
              ipAddress: 0,
              userAgent: 0,
            },
          },
        },
      },
      /*{
        $project: {
          object: {
            id: 1,
            major: 1,
            minor: 1,
            patch: 1,
            createdAt: 1,
          },
          previous: {
            object: {
              id: 1,
              major: 1,
              minor: 1,
              patch: 1,
              createdAt: 1,
            },
          },
        },
      },*/
    ])
    .map((item) => {
      return {
        ...item,
        previous: Array.isArray(item.previous) ? item.previous[0] : null,
      }
    })
    .toArray()

  const allSlides = await denormalizedSlideAudits
    .aggregate([
      {
        $lookup: {
          from: 'denormalizedSlideAudits',
          localField: 'previous',
          foreignField: '_id',
          as: 'previous',
        },
      },
      {
        $project: {
          id: 0,
          createdAt: 0,
          _createdAt: 0,
          logTime: 0,
          entity: 0,
          ipAddress: 0,
          userAgent: 0,
          tracks: {
            setting: 0,
            tags: 0,
            id: 0,
            createdAt: 0,
            _createdAt: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
          },
          previous: {
            previous: 0,
            id: 0,
            createdAt: 0,
            _createdAt: 0,
            logTime: 0,
            entity: 0,
            ipAddress: 0,
            userAgent: 0,
            tracks: {
              setting: 0,
              tags: 0,
              id: 0,
              createdAt: 0,
              _createdAt: 0,
              logTime: 0,
              entity: 0,
              ipAddress: 0,
              userAgent: 0,
            },
          },
        },
      },
    ])
    .map((item) => {
      return {
        ...item,
        previous: Array.isArray(item.previous) ? item.previous[0] : null,
      }
    })
    .toArray()

  await fs.writeFile(
    'allSlides.json',
    JSON.stringify(
      allSlides.map((s) => transformSlide2(s as unknown as SlideSchema, 0)),
      null,
      2,
    ),
    'utf-8',
  )
  await fs.writeFile('all.json', JSON.stringify(dt, null, 2), 'utf-8')
  await fs.writeFile(
    'test.json',
    JSON.stringify(
      JSON.parse(JSON.stringify(dt)).map((r: Record<string, unknown>) =>
        transformRev(r, 0),
      ),
      null,
      2,
    ),
    {
      encoding: 'utf-8',
    },
  )
}

main()
  .catch(console.error)
  .then(() => {
    process.exit(0)
  })
