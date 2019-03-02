/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvstore

import com.larskroll.common.repl._
import com.typesafe.scalalogging.StrictLogging
import fastparse.all._
import org.apache.log4j.Layout
import util.log4j.ColoredPatternLayout

import scala.concurrent.Await
import scala.concurrent.duration._

object ClientConsole {
  // Better build this statically. Has some overhead (building a lookup table).
  val simpleStr = P(CharsWhileIn(('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z'), 1).!);
  val colouredLayout = new ColoredPatternLayout("%d{[HH:mm:ss,SSS]} %-5p {%c{1}} %m%n");
}

class ClientConsole(val service: ClientService) extends CommandConsole with ParsedCommands with StrictLogging {

  import ClientConsole._;

  val getCommand = parsed(P("get" ~ " " ~ simpleStr), usage = "get <key>", descr = "Executes a GET operation for <key>.") { params =>
    val key = params;
    println(s"GET with key[$key]");

    val fr = service.get(key);
    out.println("GET sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println(s"GET complete! Response was: $r");
    } catch {
      case e: Throwable => logger.error("Error during GET.", e);
    }
  };

  val putCommand = parsed(P("put" ~ " " ~ simpleStr ~ " " ~ simpleStr), usage = "put <key> <value>", descr = "Executes a PUT operation for <key> with value <value>.") { params =>
    val (key, value) = params;
    println(s"PUT with key[$key] value[$value]");

    val fr = service.put(key, value);
    out.println("PUT Operation sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println(s"PUT complete! Response was: $r");
    } catch {
      case e: Throwable => logger.error("Error during PUT.", e);
    }
  };

  val casCommand = parsed(P("cas" ~ " " ~ simpleStr ~ " " ~ simpleStr ~ " " ~ simpleStr), usage = "cas <key> <compareValue> <setValue>", descr = "Executes a CAS operation for <key> with compare value <compareValue> and set value <setValue>.") { params =>
    val (key, compareValue, setValue) = params;
    println(s"CAS with key[$key] compareValue[$compareValue] setValue[$setValue]");

    val fr = service.cas(key, compareValue, setValue);
    out.println("CAS sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println(s"CAS complete! Response was: $r");
    } catch {
      case e: Throwable => logger.error("Error during CAS.", e);
    }
  };

  override def layout: Layout = colouredLayout;

  override def onInterrupt(): Unit = exit();

}
