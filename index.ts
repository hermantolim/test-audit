import { Db, MongoClient, ObjectId } from 'mongodb'
import { deepmerge } from 'deepmerge-ts'
import fs from 'fs/promises'

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
  seconds: number
  slideId: string
  questions: string[]
}

interface SlideObj extends Obj {
  position: number
  type: 'media' | 'elearn'
  mediaUrl: string | null
  revisionId: string
}

interface RevObj extends Obj {
  status: string
}

interface TrackSchema extends Entity<TrackObj> {
  _id: ObjectId
  _createdAt: Date
}

interface SlideSchema extends Entity<SlideObj> {
  _id: ObjectId
  _createdAt: Date
  tracks: TrackSchema[]
}

interface RevisionSchema extends Entity<RevObj> {
  _id: ObjectId
  _createdAt: Date
  previous: null | RevisionSchema
}

const sampleRev: Entity<RevObj> = {
  entity: 'LessonRevision',
  event: 'created',
  logTime: new Date('2024-07-17T16:10:06+00:00'),
  object: {
    status: 'draft',
    id: '0190c175-42fb-7ac4-9d4b-738f1515733c',
  },
  blameUser: {
    email: 'edclass_staff@edclass.com',
  },
}
const sampleSlide: Entity<SlideObj> = {
  entity: 'LessonSlide',
  event: 'created',
  logTime: new Date('2024-07-17T16:10:30+00:00'),
  object: {
    position: 1,
    type: 'media',
    mediaUrl: null,
    id: '0190c175-a3d4-7696-9238-e2a377b5f746',
    revisionId: '0190c175-42fb-7ac4-9d4b-738f1515733c',
  },
  blameUser: {
    email: 'edclass_staff@edclass.com',
  },
}
const sampleSlideUpdate: Entity<SlideObj> = {
  entity: 'LessonSlide',
  event: 'updated',
  logTime: new Date('2024-07-17T16:10:47+00:00'),
  object: {
    position: 1,
    type: 'media',
    mediaUrl:
      '/file/stream/edclass-dev-internal/lessons/11/revisions/rev-01J30QAGPRWFTNP68NB8GWSHB2/lesson-667fb8e880a912-14719366-revision-667fb8e8998d25-83035981recording-2024-06-12-162525-mp4.mp4',
    id: '0190c175-a3d4-7696-9238-e2a377b5f746',
    revisionId: '0190c175-42fb-7ac4-9d4b-738f1515733c',
  },
  blameUser: {
    email: 'edclass_staff@edclass.com',
  },
}
const sampleTrack: Entity<TrackObj>[] = [
  {
    entity: 'LessonSlideTrack',
    event: 'created',
    logTime: new Date('2024-07-17T16:11:15+00:00'),
    object: {
      id: '0190c176-5111-79a0-ba0e-65f35adf4508',
      type: 'question',
      seconds: 6,
      slideId: '0190c175-a3d4-7696-9238-e2a377b5f746',
      questions: [],
    },
    blameUser: {
      email: 'edclass_staff@edclass.com',
    },
  },
  {
    entity: 'LessonSlideTrack',
    event: 'created',
    logTime: new Date('2024-07-17T16:11:27+00:00'),
    object: {
      id: '0190c176-824e-7892-82ce-65505032ee03',
      type: 'question',
      seconds: 12,
      slideId: '0190c175-a3d4-7696-9238-e2a377b5f746',
      questions: [],
    },
    blameUser: {
      email: 'edclass_staff@edclass.com',
    },
  },
]

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

async function insertSlide(db: Db, slide: Entity<SlideObj>) {
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
  await insertDenormalized(db, rev! as any, {
    type: 'track',
    item: {
      _id: trackId.insertedId,
      ...newTrack,
    } as Entity<TrackObj>,
  })
}

async function insertDenormalized(
  db: Db,
  item:
    | Entity<RevObj>
    | (RevisionSchema & {
        slides: (SlideSchema & {
          tracks: TrackSchema[]
        })[]
      }),
  children?: {
    type: 'slide' | 'track'
    item: Entity<SlideObj> | Entity<TrackObj>
  },
) {
  const col = db.collection<
    RevisionSchema & {
      slides: (SlideSchema & {
        tracks: TrackSchema[]
      })[]
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
      ...prevBody,
      ...item,
      previous: prevBody,
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
            s.tracks = [
              ...(s.tracks || []),
              {
                ...item,
                _createdAt: new Date(),
                _id: new ObjectId(),
              },
            ]
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
async function main() {
  const client = new MongoClient('mongodb://root:root@localhost:27018')
  const db = client.db('rootdb')

  await db.createCollection('revAudits')
  await db.createCollection('slideAudits')
  await db.createCollection('trackAudits')
  const denormalizedRevAudits = await db.createCollection(
    'denormalizedRevAudits',
  )

  const newRev = await insertRev(db, sampleRev)
  const newSlide = await insertSlide(db, sampleSlide)
  const updateSlide = await insertSlide(db, sampleSlideUpdate)
  const newTrack = await insertTrack(db, sampleTrack[0])
  const newTrack2 = await insertTrack(db, sampleTrack[1])
  const deleteSlide = await insertSlide(db, sampleSlide)

  const all = await denormalizedRevAudits
    .find(
      {},
      {
        sort: {
          _createdAt: 1,
        },
      },
    )
    .toArray()
  await fs.writeFile('test.json', JSON.stringify(all, null, 2), {
    encoding: 'utf-8',
  })
}

main().catch(console.error)
