/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
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
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.memgroupby;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.trans.steps.memgroupby.MemoryGroupByData.HashEntry;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

public class MemoryGroupByAggregationNullsTest {

  static StepMockHelper<MemoryGroupByMeta, MemoryGroupByData> mockHelper;

  MemoryGroupBy step;
  MemoryGroupByData data;

  static int def = 113;

  Aggregate aggregate;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    mockHelper =
        new StepMockHelper<MemoryGroupByMeta, MemoryGroupByData>( "Memory Group By", MemoryGroupByMeta.class,
            MemoryGroupByData.class );
    when( mockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) ).thenReturn(
        mockHelper.logChannelInterface );
    when( mockHelper.trans.isRunning() ).thenReturn( true );
  }

  @Before
  public void setUp() throws Exception {
    data = new MemoryGroupByData();
    data.subjectnrs = new int[] { 0 };
    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    meta.setAggregateType( new int[] { 5 } );
    meta.setAggregateField( new String[] { "x" } );
    ValueMetaInterface vmi = new ValueMetaInteger();
    when( mockHelper.stepMeta.getStepMetaInterface() ).thenReturn( meta );
    RowMetaInterface rmi = Mockito.mock( RowMetaInterface.class );
    data.inputRowMeta = rmi;
    data.outputRowMeta = rmi;
    data.groupMeta = rmi;
    data.groupnrs = new int[] {};
    data.map = new HashMap<HashEntry, Aggregate>();
    when( rmi.getValueMeta( Mockito.anyInt() ) ).thenReturn( vmi );
    data.aggMeta = rmi;
    step = new MemoryGroupBy( mockHelper.stepMeta, data, 0, mockHelper.transMeta, mockHelper.trans );

    // put aggregate into map with default predefined value
    aggregate = new Aggregate();
    aggregate.agg = new Object[] { def };
    data.map.put( getHashEntry(), aggregate );
  }

  // test hash entry
  HashEntry getHashEntry() {
    return data.getHashEntry( new Object[data.groupMeta.size()] );
  }

  /**
   * PDI-10250 - "Group by" step - Minimum aggregation doesn't work
   * 
   * KETTLE_AGGREGATION_MIN_NULL_IS_VALUED
   * 
   * Set this variable to Y to set the minimum to NULL if NULL is within an aggregate. Otherwise by default NULL is
   * ignored by the MIN aggregate and MIN is set to the minimum value that is not NULL. See also the variable
   * KETTLE_AGGREGATION_ALL_NULLS_ARE_ZERO.
   * 
   * @throws KettleException
   */
  @Test
  public void calcAggregateResulTestMin_1_Test() throws KettleException {
    step.setMinNullIsValued( true );
    step.addToAggregate( new Object[] { null } );

    Aggregate agg = data.map.get( getHashEntry() );
    Assert.assertNotNull( "Hash code strategy changed?", agg );

    Assert.assertNull( "Value is set", agg.agg[0] );
  }

  @Test
  public void calcAggregateResulTestMin_5_Test() throws KettleException {
    step.setMinNullIsValued( false );
    step.addToAggregate( new Object[] { null } );

    Aggregate agg = data.map.get( getHashEntry() );
    Assert.assertNotNull( "Hash code strategy changed?", agg );

    Assert.assertEquals( "Value is NOT set", def, agg.agg[0] );
  }

  /**
   * Set this variable to Y to return 0 when all values within an aggregate are NULL. Otherwise by default a NULL is
   * returned when all values are NULL.
   * 
   * @throws KettleValueException
   */

  @Test
  public void getAggregateResulTestMin_0_Test() throws KettleValueException {
    // data.agg[0] is not null - this is the default behavior
    step.setAllNullsAreZero( true );
    Object[] row = step.getAggregateResult( aggregate );
    Assert.assertEquals( "Default value is not corrupted", def, row[0] );
  }

  @Test
  public void getAggregateResulTestMin_1_Test() throws KettleValueException {
    aggregate.agg[0] = null;
    step.setAllNullsAreZero( true );
    Object[] row = step.getAggregateResult( aggregate );
    Assert.assertEquals( "Returns 0 if aggregation is null", new Long( 0 ), row[0] );
  }

  @Test
  public void getAggregateResulTestMin_3_Test() throws KettleValueException {
    aggregate.agg[0] = null;
    step.setAllNullsAreZero( false );
    Object[] row = step.getAggregateResult( aggregate );
    Assert.assertNull( "Returns null if aggregation is null", row[0] );
  }
}
