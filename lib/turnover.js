#!/usr/bin/env node

'use strict'


const http = require('http')
const _ = require('lodash')
const async = require('async')
const ss = require('simple-statistics')

const config = require('../config')


const requestTheData = (queryArr, cb) => {

  let path = '/data/' + queryArr.join('/') + '?fields=TurnOvrS'

  let opts = {
    hostname: config.qwiAPI_host,
    port    : config.qwiAPI_port,
    path    : path,
  }

  let cbCalled = false
    
  let req = http.get(opts, response => {

    let body = ''

    response.on('data', d => (body += d))

    response.on('end', () => {
      try {
        let respJSON = JSON.parse(body)

        if (response.statusCode !== 200) {
          return (cbCalled !== true) && (cbCalled = true) &&
            cb(new Error((respJSON && respJSON.error) || `Error: ${response.statusCode} statusCode from server`))
        }

        return cb(null, respJSON.data)
      } catch (e) {
        console.log(e.stack)
        return cb(e) 
      }
    })
  })

  req.on('error', e => {
    console.log(e.stack)  
    return cb(e)
  })
  req.end()
}


const handleLeaf = (d) => {
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

const flattenLeaves = (d) => {
  // Because we limit the fields to TurnOvrs, the leaves should be arrays by quarter.
  if (Array.isArray(d)) {
    if (d.length > 4) {
      throw new Error('Assumption that leaf data is quarterly at most failed.')
    }
    
    return handleLeaf(d)

  } else {

    let agg = {}
    let keys = Object.keys(d)

    for (let i = 0; i < keys.length; ++i) {
      agg[keys[i]] = flattenLeaves(d[keys[i]])
    }

    return agg
  }
} 


const aggregateLeafData = (data, cb) => {
  try {
    let flattened = flattenLeaves(data)
    cb(null, flattened)
  } catch (e) {
    return cb(e)
  }
}

const getTurnoverStatistics = (queryArr, cb) => {
  let tasks = [
    requestTheData.bind(null, queryArr),
    aggregateLeafData,
  ]
  async.waterfall(tasks, cb)
} 



// If this module was run as a script.
if (require.main === module) {
  let argv = require('minimist')(process.argv.slice(2), { string: 'geography'})

  if (!argv.geography) {
    console.error('You must specify geography values')
    process.exit(1)
  }

  // Minimist treats all double-hyponated flags without equals signs as booleans
  // We want to convert them into a selected category without requested values.
  // e.g.: --agegrp results in ../agegrp/.. in the qwiAPI path.
  let reqArr = process.argv.slice(2)
                           .map(arg => arg.replace(/--/,'').replace(/=.+/,''))
                           .filter(a => a)
                           .map(a => (argv[a] === true) ? a : `${a}${argv[a]}`)

  if (!argv.year) {
    reqArr.push('year')
  }
  return getTurnoverStatistics(reqArr, (err, data) => {
    if (err) { return console.error(err.stack) }

    console.log(JSON.stringify(data, null, 4))
  })
} 


module.exports = {
  getTurnoverStatistics,  
}
