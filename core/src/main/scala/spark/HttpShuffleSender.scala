package spark

import java.io.File
import java.net.InetAddress

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.util.thread.QueuedThreadPool

import org.eclipse.jetty.rewrite.handler.{Rule =>JettyRule, RewriteHandler => JettyRewriteHandler}
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

private[spark] class ShuffleBlockRule (val subDirsPerLocalDir: Int, val localDirs: Array[File]) extends JettyRule with Logging {
  setHandling (false)
  setTerminating (false)

  override def matchAndApply(target: String, request :HttpServletRequest, response: HttpServletResponse): String = {
    val blockId = target.substring(1)
    val path = getShuffleBlockPath (blockId)
    logDebug("Rewrite Shuffle Block " + blockId + " target to " + path + ", request now is "+request )
    return path
  }

  private def getShuffleBlockPath(blockId: String): String = {
    if (!blockId.startsWith("shuffle_")) {
      throw new Exception("Block " + blockId + " is not a shuffle block")
    }
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = math.abs(blockId.hashCode)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
    val subDir = new File(localDirs(dirId), "%02x".format(subDirId))
    val file = new File(subDir, blockId)
    return file.getAbsolutePath
  }
}

class HttpShuffleSender (val port: Int, val subDirsPerLocalDir: Int, val localDirs: Array[File]) extends Logging {
  private var server: Server = null

  Runtime.getRuntime().addShutdownHook(
    new Thread() {
      override def run() {
        if (server != null) {
          server.stop()
          server = null
        }
      }
    }
  )

  def start() {
    if (server != null) {
      throw new ServerStateException("Server is already started")
    } else {
      server = new Server(port)
      val min = 1
      val max = System.getProperty("spark.shuffle.sender.threads", "40").toInt
      val threadPool = new QueuedThreadPool
      threadPool.setDaemon(true)
      threadPool.setMinThreads(min)
      threadPool.setMaxThreads(max)
      server.setThreadPool(threadPool)

      val rewriter = new JettyRewriteHandler
      rewriter.setRewriteRequestURI(false)
      rewriter.setRewritePathInfo(true)
      rewriter.setOriginalPathAttribute("requestedPath")
      val rule = new ShuffleBlockRule(subDirsPerLocalDir, localDirs)
      rewriter.addRule(rule)

      val resHandler = new ResourceHandler
      resHandler.setResourceBase("/")

      val debugHandler = new DebugHandler

      val handlerList = new HandlerList
      //handlerList.setHandlers(Array(debugHandler, rewriter,debugHandler, resHandler, debugHandler, new DefaultHandler))
      handlerList.setHandlers(Array(rewriter, resHandler, new DefaultHandler))
      server.setHandler(handlerList)
      server.start()
      server.join()
    }
  }
}


private[spark] class DebugHandler extends AbstractHandler with Logging
{
    def handle(target: String, baseRequest: Request, request: HttpServletRequest, response:HttpServletResponse) = {
        baseRequest.setHandled(false)
        logDebug("target: "+ target +", BaseRequest: ( "+baseRequest.getRequestURI() + "," + baseRequest.getPathInfo() +"), ServletRequest: "+ request + ", Response: "+response)
    }
}

private[spark] object HttpShuffleSender {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: HttpShuffleSender <port> <subDirsPerLocalDir> <list of shuffle_block_directories>")
      System.exit(1)
    }
    val port = args(0).toInt
    val subDirNum = args(1).toInt
    val localDirs = args.zipWithIndex.filter(_._2 > 1).map(x=>new File(x._1))
    val sender = new HttpShuffleSender (port, subDirNum, localDirs)
    sender.start
  }
}
