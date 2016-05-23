#!/usr/bin/env node

'use strict'


const http = require('http')
const _ = require('lodash')
const async = require('async')
const ss = require('simple-statistics')

const config = require('../config')


const requestTheData = (queryObj, cb) => {

  let pathChunks = ['/data']
    
  let geography = queryObj.geography

  if (!geography) {
    return cb(new Error('You must specify geography values'))
  }

  pathChunks.push(`geography${geography.replace(/\s/,'')}`)

  pathChunks.push(`year${(queryObj.year) ? queryObj.year : ''}`)

  let remainingQueryObj = _.omit(queryObj, ['geography', 'year'])

  pathChunks = pathChunks.concat(_.map(remainingQueryObj, (reqVals,cat) => `${cat}${reqVals}`))
      
  let path = pathChunks.join('/') + '?fields=TurnOvrS'

console.log(path)

  let opts = {
    hostname: config.qwiAPI_host,
    port    : config.qwiAPI_port,
    path    : path,
  }

  let req = http.get(opts, response => {

    var body = ''
    response.on('data', d => (body += d))

    response.on('end', () => {
      queryObj.data = JSON.parse(body).data

      return cb(null, queryObj)
    })
  })

  req.on('error', cb)
  req.end()
}

const aggregateLeafData = (queryObj, cb) => {

  let f = (d) => {
    // Because we limit the fields to TurnOvrs, the leaves should be arrays by quarter.
    if (Array.isArray(d)) {
      if (d.length > 4) {
        cb(new Error('Assumption that leaf data is quarterly at most failed.'))
      }

      // Client requested quarterly figures.
      if (d.length === 1) {
        return d[0] 
      }

      let agg = _.omit(d[0], ['turnovrs', 'quarter'])

      let turnovrs = d.map(o => o.turnovrs)

      agg.turnovrs_mean = ss.mean(turnovrs)
      agg.turnovrs_median = ss.median(turnovrs)
      agg.turnovrs_mad = ss.mad(turnovrs)
      agg.turnovrs_standardDeviation = ss.standardDeviation(turnovrs)

      agg.turnovrs_quarterly = new Array(4).fill(null)

      for (let i = 0; i < d.length; ++i) {
        agg.turnovrs_quarterly[parseInt(d[i].quarter)-1] = d[i].turnovrs
      }

      return agg
    }

    let agg = {}
    let keys = Object.keys(d)

    for (let i = 0; i < keys.length; ++i) {
      agg[keys[i]] = f(d[keys[i]])
    }

    return agg
  } 

  let periodicity = (queryObj.periodicity) ? queryObj.periodicity.toUpperCase() : 'a'

  if (periodicity === 'Q') {
    return cb(null, queryObj.data)
  } else {
    return cb(null, f(queryObj.data))
  }

}

const getTurnoverStatistics = (queryObj, cb) => {
  let tasks = [
    requestTheData.bind(null, queryObj),
    aggregateLeafData,
  ]
  async.waterfall(tasks, cb)
} 

module.exports = {
  getTurnoverStatistics,  
}



// If this module was run as a script.
if (require.main === module) {
  let argv = require('minimist')(process.argv.slice(2), { string: 'geography'})

  // Minimist treats all double-hyponated flags without equals signs as booleans
  // We want to convert them into a selected category without requested values.
  // e.g.: --agegrp results in ../agegrp/.. in the path.
  argv = _.reduce(argv, (acc, val, key) => {
    if (val === true) {
      acc[key] = ''
    } 

    return acc
  }, argv)

  let reqObj = _.omit(argv, '_')

  return getTurnoverStatistics(reqObj, (err, data) => {
    if (err) { return console.error(err.stack) }

    console.log(JSON.stringify(data, null, 4))
  })
} 
