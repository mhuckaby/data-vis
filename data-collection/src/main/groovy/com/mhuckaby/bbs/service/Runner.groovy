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

class Runner {
    // TODO Add arg for specifying output file vs hardcoded output.json
    static String commandParseBbs = "parse-bbs"
    static String commandParseSpark = "parse-spark-out"
    static void main(String[] args) {

        if(args.length != 2) {
            println "arg[0] to be command in:\n" +
                    "\t$commandParseBbs\n" +
                    "\t$commandParseSpark\n" +
                    "arg[1] should be a dir or file path\n" +
                    "i.e. ./gradlew run --args='parse-bbs /file/location'"

        }else {
            switch(args[0]) {
                case commandParseBbs:
                    new Runner().executeParseBbs(args[1])
                    break
                case commandParseSpark:
                    new Runner().executeParseSpark(args[1])
                    break
                default:
                    println "Unrecognized command"
            }
        }
    }

    def executeParseBbs(path) {
        DataMgr dataMgr = new DataMgr()
        println("executing $commandParseBbs")
        dataMgr.parseDir(path, 'output.json')
    }
    
    def executeParseSpark(path) {
        DataMgr dataMgr = new DataMgr()
        println("executing $commandParseSpark ")
        dataMgr.parseSparkOut(path)
    }
}
