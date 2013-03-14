package spark.network.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundByteHandlerAdapter;
import io.netty.util.CharsetUtil;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public abstract class FileClientHandler extends ChannelInboundByteHandlerAdapter {

  private FileHeader currentHeader = null;

  public abstract void handle(ChannelHandlerContext ctx, ByteBuf in, FileHeader header);

  @Override
  public ByteBuf newInboundBuffer(ChannelHandlerContext ctx) {
    // Use direct buffer if possible.
    return ctx.alloc().ioBuffer();
  }
  
  @Override
  public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in) {
    // get header
    if (currentHeader == null && in.readableBytes() >= FileHeader.HEADER_SIZE()) {
      currentHeader = FileHeader.create(in.readBytes(FileHeader.HEADER_SIZE()));
    }
    // get file
    if(in.readableBytes() >= currentHeader.fileLen()){
      handle(ctx,in,currentHeader);
      currentHeader = null;
    }
  }

  public void closeContext(ChannelHandlerContext ctx){
    ctx.close();
  }
}

