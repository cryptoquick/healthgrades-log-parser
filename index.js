const got = require('got')
const _ = require('highland')
const path = require('path')

// Utilities

// https://www.w3.org/Daemon/User/Config/Logging.html#common-logfile-format
const parseLine = (line) => {
  const [
    remotehost = '',
    rfc931 = '',
    authuser = '',
    datetime = '',
    tz = '',
    method = '',
    pathname = '',
    protocol = '',
    status = '',
    bytes = '',
  ] = line.replace(/["\[\]]/g, '').split(' ')

  return {
    remotehost,
    rfc931,
    authuser,
    datetime,
    tz,
    method,
    pathname,
    protocol,
    status,
    bytes,
  }
}

const count = [0, (s) => ++s] // Note flipped memo argument. Reversed in next major release.
const sumReducer = (acc, s) => acc + s

const log = (msg) => (result) => {
  console.log(`${msg}: ${result}`)
  return result
}

const logBy = (msg, accessor) => (result) => {
  console.log(`${msg}: ${result[accessor]}`)
  return result
}

// Filters
const countByTerm = (term) => (line) => line.includes(term)
const countByDate = (month, day, year) => (line) =>
  parseLine(line).datetime.includes(`${day}/${month}/${year}`) // ex: 17/May/2015
const countPathByExtensions = (dir, extensions) => (line) =>
  line.includes(' ' + dir) &&
  extensions.includes(path.extname(parseLine(line).pathname))

// Reducers
const dailyAverage = () => [{
    lastDate: null,
    counts: [0],
    average: 0,
  },
  ({
    lastDate,
    counts,
    average,
  }, line) => {
    // handle blank line
    if (line.length === 0) {
      return {
        lastDate,
        counts,
        average,
      }
    }

    const {
      datetime
    } = parseLine(line)
    const date = datetime.split(':')[0]

    // Increment last count
    counts[counts.length - 1] = counts[counts.length - 1] + 1

    // New day
    if (date !== (lastDate || date)) {
      counts.push(0)
    }

    average = counts.reduce(sumReducer, 0) / counts.length

    return {
      lastDate: date,
      counts,
      average,
    }
  }
]

// Queries
const filters = {
  'How many times did “Googlebot/2.1” fetch resources?': countByTerm(
    'Googlebot/2.1'
  ),
  'How many transactions took place on May 18, 2015?': countByDate(
    'May',
    18,
    2015
  ),
  'How many images loaded from /images?': countPathByExtensions('/images', [
    '.jpeg',
    '.jpg',
    '.png',
    '.gif',
  ]),
}

const reducers = {
  'Average number of resources loaded per day?': dailyAverage(),
}

// Streams
const httpReader = got.stream('https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs')
const splitStream = _(httpReader).split()

for (f in filters) {
  const filter = filters[f]
  const forkedStream = splitStream.fork()
  forkedStream.resume()
  forkedStream.filter(filter).reduce(...count).each(log(f))
}

for (r in reducers) {
  const reducer = reducers[r]
  const forkedStream = splitStream.fork()
  forkedStream.resume()
  forkedStream.reduce(...reducer).each(logBy(r, 'average'))
}
