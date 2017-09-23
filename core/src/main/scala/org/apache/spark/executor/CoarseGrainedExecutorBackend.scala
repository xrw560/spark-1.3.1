/*
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

package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.concurrent.Await

import akka.actor.{Actor, ActorSelection, Props}
import akka.pattern.Patterns
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, SignalLogger, Utils}

/**
  * worker中为application启动的executor，实际上是启动了这个CoarseGrainedExecutorBackend进程
  */
private[spark] class CoarseGrainedExecutorBackend(
        driverUrl: String,
        executorId: String,
        hostPort: String,
        cores: Int,
        userClassPath: Seq[URL],
        env: SparkEnv)
        extends Actor with ActorLogReceive with ExecutorBackend with Logging {

    Utils.checkHostPort(hostPort, "Expected hostport")

    var executor: Executor = null
    var driver: ActorSelection = null

    //TODO CoarseGrainedExecutorBackend的生命周期方法
    /**
      * 在actor的初始化方法中
      */
    override def preStart() {
        logInfo("Connecting to driver: " + driverUrl)
        //TODO 跟Driver建立连接
        // 获取了driver的actor
        driver = context.actorSelection(driverUrl)
        //TODO Executor向DriverActor发送消息，来注册Exectuor
        // 向driver发送RegisterExecutor消息
        driver ! RegisterExecutor(executorId, hostPort, cores, extractLogUrls)
        context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    def extractLogUrls: Map[String, String] = {
        val prefix = "SPARK_LOG_URL_"
        sys.env.filterKeys(_.startsWith(prefix))
                .map(e => (e._1.substring(prefix.length).toLowerCase, e._2))
    }

    override def receiveWithLogging = {
        //TODO DirverActor发送给Executor的消息，告诉她已经注册成功
        // driver注册executor成功之后，会发送回来RegisteredExecutor消息
        // 此时，CoarseGrainedExecutorBackend，会创建Executor对象，作为执行句柄
        // 其实它的大部分功能，都是通过Executor实现的
        case RegisteredExecutor =>
            logInfo("Successfully registered with driver")
            val (hostname, _) = Utils.parseHostPort(hostPort)
            //TODO 创建了一个Executor实例，用来执行业务逻辑
            executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)

        case RegisterExecutorFailed(message) =>
            logError("Slave registration failed: " + message)
            System.exit(1)

        //TODO DirverActor发送给Executor的消息，让Executor启动计算任务
            // 启动task
        case LaunchTask(data) =>
            if (executor == null) {
                logError("Received LaunchTask command but executor was null")
                System.exit(1)
            } else {
                //获得序列化器
                // 反序列化task
                val ser = env.closureSerializer.newInstance()
                //TODO 反序列化Task
                val taskDesc = ser.deserialize[TaskDescription](data.value)
                logInfo("Got assigned task " + taskDesc.taskId)
                //TODO 将反序列化后的Task放到线程池里面
                // 用内部的执行句柄,Executor，的launchTask()方法，来启动一个task
                executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
                    taskDesc.name, taskDesc.serializedTask)
            }

        case KillTask(taskId, _, interruptThread) =>
            if (executor == null) {
                logError("Received KillTask command but executor was null")
                System.exit(1)
            } else {
                executor.killTask(taskId, interruptThread)
            }

        case x: DisassociatedEvent =>
            if (x.remoteAddress == driver.anchorPath.address) {
                logError(s"Driver $x disassociated! Shutting down.")
                System.exit(1)
            } else {
                logWarning(s"Received irrelevant DisassociatedEvent $x")
            }

        case StopExecutor =>
            logInfo("Driver commanded a shutdown")
            executor.stop()
            context.stop(self)
            context.system.shutdown()
    }

    override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
        driver ! StatusUpdate(executorId, taskId, state, data)
    }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {

    private def run(
            driverUrl: String,
            executorId: String,
            hostname: String,
            cores: Int,
            appId: String,
            workerUrl: Option[String],
            userClassPath: Seq[URL]) {

        SignalLogger.register(log)

        SparkHadoopUtil.get.runAsSparkUser { () =>
            // Debug code
            Utils.checkHost(hostname)

            // Bootstrap to fetch the driver's Spark properties.
            val executorConf = new SparkConf
            val port = executorConf.getInt("spark.executor.port", 0)
            //TODO 在Executor里创建ActorSystem
            val (fetcher, _) = AkkaUtils.createActorSystem(
                "driverPropsFetcher",
                hostname,
                port,
                executorConf,
                new SecurityManager(executorConf))
            //TODO 跟Driver建立连接
            val driver = fetcher.actorSelection(driverUrl)
            val timeout = AkkaUtils.askTimeout(executorConf)
            val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
            val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]] ++
                    Seq[(String, String)](("spark.app.id", appId))
            fetcher.shutdown()

            // Create SparkEnv using properties we fetched from the driver.
            val driverConf = new SparkConf()
            for ((key, value) <- props) {
                // this is required for SSL in standalone mode
                if (SparkConf.isExecutorStartupConf(key)) {
                    driverConf.setIfMissing(key, value)
                } else {
                    driverConf.set(key, value)
                }
            }
            val env = SparkEnv.createExecutorEnv(
                driverConf, executorId, hostname, port, cores, isLocal = false)

            // SparkEnv sets spark.driver.port so it shouldn't be 0 anymore.
            val boundPort = env.conf.getInt("spark.executor.port", 0)
            assert(boundPort != 0)

            // Start the CoarseGrainedExecutorBackend actor.
            val sparkHostPort = hostname + ":" + boundPort
            //TODO CoarseGrainedExecutorBackend真正进行通信的Actor
            env.actorSystem.actorOf(
                Props(classOf[CoarseGrainedExecutorBackend],
                    driverUrl, executorId, sparkHostPort, cores, userClassPath, env),
                name = "Executor")
            workerUrl.foreach { url =>
                env.actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
            }
            env.actorSystem.awaitTermination()
        }
    }

    //TODO 启动Executor子进程的入口
    def main(args: Array[String]) {
        var driverUrl: String = null
        var executorId: String = null
        var hostname: String = null
        var cores: Int = 0
        var appId: String = null
        var workerUrl: Option[String] = None
        val userClassPath = new mutable.ListBuffer[URL]()

        var argv = args.toList
        while (!argv.isEmpty) {
            argv match {
                case ("--driver-url") :: value :: tail =>
                    driverUrl = value
                    argv = tail
                case ("--executor-id") :: value :: tail =>
                    executorId = value
                    argv = tail
                case ("--hostname") :: value :: tail =>
                    hostname = value
                    argv = tail
                case ("--cores") :: value :: tail =>
                    cores = value.toInt
                    argv = tail
                case ("--app-id") :: value :: tail =>
                    appId = value
                    argv = tail
                case ("--worker-url") :: value :: tail =>
                    // Worker url is used in spark standalone mode to enforce fate-sharing with worker
                    workerUrl = Some(value)
                    argv = tail
                case ("--user-class-path") :: value :: tail =>
                    userClassPath += new URL(value)
                    argv = tail
                case Nil =>
                case tail =>
                    System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
                    printUsageAndExit()
            }
        }

        if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
                appId == null) {
            printUsageAndExit()
        }
        //TODO 调用RUN方法
        run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
    }

    private def printUsageAndExit() = {
        System.err.println(
            """
              |"Usage: CoarseGrainedExecutorBackend [options]
              |
              | Options are:
              |   --driver-url <driverUrl>
              |   --executor-id <executorId>
              |   --hostname <hostname>
              |   --cores <cores>
              |   --app-id <appid>
              |   --worker-url <workerUrl>
              |   --user-class-path <url>
              |""".stripMargin)
        System.exit(1)
    }

}
