/*
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 * **************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pentaho.di.core.logging;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import junit.framework.Assert;

import org.junit.Test;
import org.pentaho.di.core.Const;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggingBufferTest {

  @Test
  public void testRaceCondition() throws Exception {

    final int eventCount = 100;

    final LoggingBuffer buf = new LoggingBuffer( 200 );

    final AtomicBoolean done = new AtomicBoolean( false );

    final KettleLoggingEventListener lsnr = new KettleLoggingEventListener() {
      @Override public void eventAdded( KettleLoggingEvent event ) {
        //stub
      }
    };

    final KettleLoggingEvent event = new KettleLoggingEvent();

    final CountDownLatch latch = new CountDownLatch( 1 );

    Thread.UncaughtExceptionHandler errorHandler = new Thread.UncaughtExceptionHandler() {
      @Override public void uncaughtException( Thread t, Throwable e ) {
        e.printStackTrace();
      }
    };

    Thread addListeners = new Thread( new Runnable() {
      @Override public void run() {
        try {
          while ( !done.get() ) {
            buf.addLoggingEventListener( lsnr );
          }
        } finally {
          latch.countDown();
        }
      }
    }, "Add Listeners Thread" ) {

    };

    Thread addEvents = new Thread( new Runnable() {
      @Override public void run() {
        try {
          for ( int i = 0; i < eventCount; i++ ) {
            buf.addLogggingEvent( event );
          }
          done.set( true );
        } finally {
          latch.countDown();
        }
      }
    }, "Add Events Thread" ) {

    };

    // add error handlers to pass exceptions outside the thread
    addListeners.setUncaughtExceptionHandler( errorHandler );
    addEvents.setUncaughtExceptionHandler( errorHandler );

    // start
    addListeners.start();
    addEvents.start();

    // wait both
    latch.await();

    // check
    Assert.assertEquals( "Failed", true, done.get() );

  }

  @Test
  public void testBufferSizeRestrictions() {
    final LoggingBuffer buff = new LoggingBuffer( 10 );

    assertEquals( 10, buff.getMaxNrLines() );
    assertEquals( 0, buff.getLastBufferLineNr() );
    assertEquals( 0, buff.getNrLines() );

    // Load 20 records.  Only last 10 should be kept
    for( int i = 1; i <= 20; i++ ) {
      buff.addLogggingEvent(
        new KettleLoggingEvent( "Test #" + i + Const.CR + "Hello World!", Long.valueOf( i ), LogLevel.DETAILED ) );
    }
    assertEquals( 10, buff.getNrLines() );

    // Check remaining records, confirm that they are the proper records
    int i = 11;
    Iterator<BufferLine> it = buff.getBufferIterator();
    assertNotNull( it );
    while( it.hasNext() ) {
      BufferLine bl = it.next();
      assertNotNull( bl.getEvent() );
      assertEquals( "Test #" + i + Const.CR + "Hello World!", bl.getEvent().getMessage() );
      assertEquals( Long.valueOf( i ).longValue(), bl.getEvent().getTimeStamp() );
      assertEquals( LogLevel.DETAILED, bl.getEvent().getLevel() );
      i++;
    }
    assertEquals( i, 21 ); // Confirm that only 10 lines were iterated over

    assertEquals( 0, buff.getBufferLinesBefore( 10L ).size() );
    assertEquals( 5, buff.getBufferLinesBefore( 16L ).size() );
    assertEquals( 10, buff.getBufferLinesBefore( System.currentTimeMillis() ).size() );

    buff.clear();
    assertEquals( 0, buff.getNrLines() );
    it = buff.getBufferIterator();
    assertNotNull( it );
    while( it.hasNext() ) {
      fail( "This should never be reached, as the LogBuffer is empty" );
    }
  }
}
