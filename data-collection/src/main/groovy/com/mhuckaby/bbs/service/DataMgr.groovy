/**
 * Copyright 2018 Matthew Huckaby
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mhuckaby.bbs.service

import com.mhuckaby.bbs.domain.BbsRecord
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

/**
 * DataMgr contains methods to support data collection and data pre-processing.
 */
class DataMgr {
    // Pattern - Date
    def patternDate = /(\d{4})/

    // Pattern - Date (single year)
    def patternDateSingle = /.*\($patternDate\)/

    // Pattern - Date range
    def patternDateRange = /.*\($patternDate-$patternDate\)/

    // Pattern - Line separator
    def pLineSeparator = /-{14}\+-{59}/

    // Pattern - Line containing location and duration range (years active)
    def pLineLocationDurationRange = /.*\| (.*), ([A-Z]{2}) $patternDateRange/

    // Pattern - Line containing location and duration range (years active)
    def pLineLocationDurationSingle = /.*\| (.*), ([A-Z]{2}) $patternDateSingle/

    // Pattern - Line containing phone and BBS name values
    def pPhoneName = /.(\d{3}-\d{3}-\d{4}).*\| (.*)/

    // Pattern - Line containing BBS software
    def pSoftware = /.*SOFTWARE \| (.*)/

    // Pattern - Line containing Sysop
    def pSysop = /.*SYSOP \| (.*)/

    // Patterns and the group:field mapping on BbsRecord class
    def registeredPatterns = [
        registerPattern(pLineSeparator),
        registerPattern(pPhoneName, ['phone', 'name']),
        registerPattern(pLineLocationDurationRange, ['city', 'state', 'start', 'end']),
        registerPattern(pLineLocationDurationSingle, ['city', 'state', 'start']),
        registerPattern(patternDateSingle, ['start']),
        registerPattern(patternDateRange, ['start', 'end']),
        registerPattern(pSoftware, ['software']),
        registerPattern(pSysop, ['sysop']),
    ]

    /**
     * Registers a regular expression pattern and the fields on BbsRecord added in the
     * order in which they appear as groups in the regular expression.
     * @param pattern
     * @param groupToFieldMappings
     * @return Lambda
     */
    def registerPattern(pattern, groupToFieldMappings = []) {
        return { bbsRecord, line ->
            def matcher = (line =~ pattern)

            if(!matcher.matches()) {
                return false
            }else if(!groupToFieldMappings) {
                return true
            }

            groupToFieldMappings.eachWithIndex { field, i ->
                // [0][0] = full matching text
                // [0][1] = matched group value
                bbsRecord."$field" = matcher[0][i + 1].trim()
            }

            return false
        }
    }

    /**
     * Parse an entire directory of files
     * @param filePath
     * @return List<BbsRecord>
     */
    def parseDir(filePath, outputFilename = "output.json") {
        File data = new File(filePath)
        File out = new File("$filePath/$outputFilename")
        data.listFiles().each {
            def records = parseFile(it)
            records.each {
                if(it.phone) {
                    out.append("${new JsonBuilder(it).toString()}\n")
                }
            }
        }
        println "wrote: ${out.getAbsolutePath()}"
    }

    /**
     * Parse a single file
     * @param file
     * @return List<BbsRecord>
     */
    def parseFile(file) {
        List<BbsRecord> bbsRecords = []
        BbsRecord bbsRecord = new BbsRecord()

        file.newReader().lines().forEach({ line ->
            this.registeredPatterns.inject(false) { previous, current ->
                if(previous) {
                    bbsRecords.push(bbsRecord)
                    bbsRecord = new BbsRecord()
                    false
                }else {
                    current(bbsRecord, line)
                }
            }
        })

        bbsRecords
    }

    /**
     * Parses the output of a Spark query/report and formats for use with front-end.
     * @param file
     */
    def parseSparkOut(filePath) {
        File data = new File(filePath)
        def result = []
        data.newReader().lines().forEach({
          def object = new JsonSlurper().parseText(it)
          result.push(object)
        })
        println new JsonBuilder(result).toPrettyString()
    }

}
