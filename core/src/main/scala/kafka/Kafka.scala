/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.util.Properties

import joptsimple.OptionParser
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Logging}
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    val optionParser = new OptionParser
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])

    if (args.length == 0) {
      CommandLineUtils.printUsageAndDie(optionParser, "USAGE: java [options] %s server.properties [--override property=value]*".format(classOf[KafkaServer].getSimpleName()))
    }

    val props = Utils.loadProps(args(0))

    if(args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if(options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props.putAll(CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt)))
    }
    props
  }

  def main(args: Array[String]): Unit = {
    try {
      // 根据命令行的参数，获取配置文件中的相关配置,这里获取到的也就是/usr/local/etc/kafka/server.properties的内容
      // 这里同时还会检查是否有入参，如果没有就会报错
      val serverProps = getPropsFromArgs(args)
      // 根据配置构造一个kafkaServerStartable对象,这里面会检验必要的参数是否有值
      val kafkaServerStartable = KafkaServerStartable.fromProps(serverProps)

      // attach shutdown handler to catch control-c
      // 注册 shutdown 钩子函数 绑定一个进程关闭的钩子
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          kafkaServerStartable.shutdown
        }
      })

      // 在KafkaServerStartable.scala的startup方法中,会继续调用KafkaServer#startup()方法
      // 在KafkaServer#startup()方法中，开始初始化并加载各个组件
      kafkaServerStartable.startup

      // 阻塞直到kafka被关闭
      // 底层用了java的CountDownLatch.await()。当kafka被关闭时，对应的CountDownLatch.countDown()方法会被调用,这时候程序就会真正退出
      kafkaServerStartable.awaitShutdown
    }
    catch {
      case e: Throwable =>
        fatal(e)
        System.exit(1)
    }
    System.exit(0)
  }
}
