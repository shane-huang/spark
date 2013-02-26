package spark

import spark.network.ConnectionManagerId
import java.net.{URL,URLConnection}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import java.nio.charset.{Charset,CharsetDecoder}
import org.eclipse.jetty.http.HttpFields
import org.eclipse.jetty.http.HttpHeaders

private[spark] class HttpShuffleCopier extends Logging {

  val shuffleConnectionTimeout = System.getProperty("spark.shuffle.copier.connection.timeout.ms", "60000").toInt
  val shuffleReadTimeout = System.getProperty("spark.shuffle.copier.read.timeout.ms", "60000").toInt

  def getUrl(cmId:ConnectionManagerId, blockId: String) = {
    val urlstr = "http://"+cmId.host + ":" + cmId.port+"/"+blockId
    logDebug("Block URL is: "+urlstr)
    new URL(urlstr)
  }

  private def openConnection(cmId: ConnectionManagerId, blockId: String) = {
    val url = getUrl(cmId,blockId)
    url.openConnection()
  }

  private def getInputStream(connection: URLConnection) = {
    connection.setReadTimeout(shuffleReadTimeout)
    connection.setConnectTimeout(shuffleConnectionTimeout)
    connection.getInputStream()
  }

  def getBlock(cmId: ConnectionManagerId, blockId: String, blockSize:Long): ByteBuffer = {

    def parseInt(s: String) = try { Some(s.toInt) } catch { case _ => None }

    val connection = openConnection(cmId,blockId)

    val contentLen = parseInt(connection.getHeaderField(HttpHeaders.CONTENT_LENGTH)).getOrElse(0)
    
    logDebug("http response reported block " + blockId + " is of size " + contentLen + " bytes")

    //we read from the HTTP response header for the size of block
    //if content length is not available we use 120% blockSize
    val bufferSize = if ( contentLen > 0 ) contentLen else (blockSize * 1.2).toInt
    logDebug("Estimated blockSize = " + blockSize+" , bufferSize = " + bufferSize)
    val data = new Array[Byte](bufferSize)
    val input = getInputStream(connection)
    var bytesRead = 0
    try {
      var n = input.read(data, 0, bufferSize)
      while (n > 0) {
        bytesRead += n;
        n = input.read(data, bytesRead, (bufferSize-bytesRead))
      }
      input.close()
    } catch {
      case _ => throw new SparkException("Error getting Block " + blockId)
    }
    val byteBuffer = ByteBuffer.wrap(data)
    byteBuffer.limit(bytesRead)
    byteBuffer
  }

  def getBlocks( cmId: ConnectionManagerId,
                  blocks: Seq[(String, Long)],
                  put: (String, Long, ByteBuffer) => Unit ) = {
    blocks.map {
      case (blockId, size) => {
        val blockData = getBlock(cmId,blockId,size)
        put(blockId,size,blockData)
      }
    }
  }

}


private[spark] object HttpShuffleCopier extends Logging {

  def main(args: Array[String]){
    if (args.length < 4) {
      println("Usage: HttpShuffleCopier <host> <port> <blockId> <block size>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val blockId = args(2)
    val blockSize = args(3).toLong

    val cmId = new ConnectionManagerId(host,port)
    logInfo("Coping Block "+ blockId + " of size " + blockSize + " from : "+cmId)
    val copier = new HttpShuffleCopier
    val block = copier.getBlock(new ConnectionManagerId(host,port),blockId, blockSize)
    logInfo("Got block " + blockId + ", status : " + block)

    val charset = Charset.forName("UTF-8")
    val decoder = charset.newDecoder()
    val contents = decoder.decode(block).toString()
    logInfo("Block contents : \"" + contents+"\"")
  }
}
