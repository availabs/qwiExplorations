#!/usr/bin/env node

const fs = require('fs')
const path = require('path')

const http = require('http')

const config = require('../config')

const async = require('async')

const _ = require('lodash')


const qwiGeographiesByFipsCode = JSON.parse(fs.readFileSync('./static/qwiGeographiesByFipsCode.json'))


const once = f => 
  (err, data, desc) => {
    if (desc) { console.log(desc) }
    f(err, data) 
    f = () => {}
  }


const requestTheData = (geographies, cb) => {

  let opts = _.clone(config)

  opts.path = `/data/${geographies}/year/quarter/industry/firmage01` + 
              `?fields=emp&fields=hira&fields=payroll&fields=turnovrs&fields=frmjbc`

  cb = once(cb)


  return http.get(opts, response => {

    response.on('close', () => {
      cb(new Error('Response closed'), null, 'on close')
    })

    response.setTimeout(300000, () => {
      cb(new Error('Response Timed out'), null, 'response timeout')
    })

    let body = ''
    response.on('data', d => (body += d))

    response.on('end', () => {
      try {
        let respJSON = JSON.parse(body)

        if (response.statusCode !== 200) {
          let err = new Error((respJSON && respJSON.error) || `Error: ${response.statusCode} statusCode from server`)
          return cb(err, null, `status code = ${response.statusCode}`)
        }

        return cb(null, respJSON.data)
      } catch (e) {
        console.error(e.stack)
        return cb(e, null, 'Caught error') 
      }
    })

    response.on('error', e => cb(e, null, 'request error caught'))

  }).on('error', e => cb(e, null, 'request error caught'))
    .end()
}


const collector = {}

const computeVibrancyStats = (data, cb) =>  {

  return cb(null, _.mapValues(data, (years, geography) => 
              _.mapValues(years, (quarters, year) => 
                _.mapValues(quarters, (sectors, quarter) => 
                  _.mapValues(sectors, (firmages, sector) => {

                    let msa = geography.slice(2)

                    let emp, empRatio, hira, hiraRatio, payrollRatio, sectorChurn

                    if (firmages['1']) {

                      emp          = firmages['1'][0].emp
                      empRatio     = firmages['1'][0].emp/firmages['0'][0].emp
                      hiraRatio    = firmages['1'][0].hira/firmages['0'][0].hira
                      payrollRatio = firmages['1'][0].payroll/firmages['0'][0].payroll
                      frmjbcRatio  = firmages['1'][0].frmjbc/firmages['0'][0].frmjbc
                      sectorChurn  = firmages['0'][0].turnovrs

                      let colNode = [year, quarter, sector].reduce((a,v) => (a[v] || (a[v]={})), collector)

                      if (Number.isFinite(empRatio)) {
                        (colNode.empRatio || (colNode.empRatio = [])).push({msa, value: empRatio})
                      } else {
                        empRatio = null
                      }

                      if (Number.isFinite(hiraRatio)) {
                        (colNode.hiraRatio || (colNode.hiraRatio = [])).push({msa, value: hiraRatio})
                      } else {
                        hiraRatio = null
                      }

                      if (Number.isFinite(frmjbcRatio)) {
                        (colNode.frmjbcRatio || (colNode.frmjbcRatio = [])).push({msa, value: frmjbcRatio})
                      } else {
                        frmjbcRatio = null
                      }

                      if (Number.isFinite(payrollRatio)) {
                        (colNode.payrollRatio || (colNode.payrollRatio = [])).push({msa, value: payrollRatio})
                      } else {
                        payrollRatio = null
                      }

                      if (Number.isFinite(sectorChurn)) {
                        (colNode.sectorChurn || (colNode.sectorChurn = [])).push({msa, value: sectorChurn})
                      } else {
                        sectorChurn = null
                      }

                    } else {
                      empRatio = null
                      hiraRatio = null
                      payrollRatio = null
                    }

                    return {
                      msa,
                      year,
                      sector,
                      emp,
                      empRatio,
                      hira,
                      hiraRatio,
                      frmjbcRatio,
                      payrollRatio,
                      sectorChurn,
                    }
                  })
                )
              )
            )
        )
}



const getVibrancyStats = (acc, fipsCode, cb) => {

  console.time(fipsCode)

  let dynamicQueryRoute = `geography${qwiGeographiesByFipsCode[fipsCode].join('')}`

  let tasks = [
    requestTheData.bind(null, dynamicQueryRoute),
    computeVibrancyStats,
  ]

  async.waterfall(tasks, (err, data) => {
    console.timeEnd(fipsCode)
    if (err) { return cb(err) }

    _.forEach(data, (vibrancyStats, geography) => {
      let msaCode = geography.slice(2)

      acc[msaCode] = vibrancyStats
    })

    return cb(null, acc)
  })
} 


const applyTheRankings =(result)=> {

  let f = (o, p) => {

    if (Array.isArray(o)) {
      o.sort((a,b) => (b.value - a.value))

      let rank = 1
      let prevVal = Number.NEGATIVE_INFINITY

      o.forEach((d,i) => {
        if (d.value !== prevVal) {
          rank = (i + 1)
        } 
        prevVal = d.value

        let path = [d.msa].concat(p).join('.') + 'Rank'
        _.set(result, path, rank)
      })

    } else {
      _.forEach(o, (v, k) => f(v, p.concat([k])))
    }
  }

  f(collector, [])
}



let fipsCodes = Object.keys(qwiGeographiesByFipsCode).sort()

//fipsCodes = [fipsCodes[0]]

async.reduce(fipsCodes, {}, getVibrancyStats, (err, result) => {

  if (err) {
    return console.error(err.stack || err)
  } 

  applyTheRankings(result)

  fs.writeFileSync('startupActivityByIndustry.json', JSON.stringify(result))
})
