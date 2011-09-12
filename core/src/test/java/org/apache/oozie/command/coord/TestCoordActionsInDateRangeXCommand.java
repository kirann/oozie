/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.util.Date;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.test.XDataTestCase;
import org.apache.oozie.util.DateUtils;

public class TestCoordActionsInDateRangeXCommand extends XDataTestCase {

    private Services services;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        services = new Services();
        services.init();
        cleanUpDBTables();
        LocalOozie.start();
    }

    @Override
    protected void tearDown() throws Exception {
        LocalOozie.stop();
        services.destroy();
        super.tearDown();
    }

    public void testCoordActionsInDateRange() throws CommandException {
        try {
            int actionNum = 1;
            CoordinatorJobBean job = addRecordToCoordJobTable(CoordinatorJob.Status.RUNNING, false, false);
            CoordinatorActionBean actionId1 = addRecordToCoordActionTable(job.getId(), actionNum,
                    CoordinatorAction.Status.SUCCEEDED, "coord-action-get.xml", 0);
            Date nominalTime = actionId1.getNominalTime();
            CoordActionsInDateRangeXCommand coordActionsInDateRangeXCommandObj = new CoordActionsInDateRangeXCommand(
                    "CoordActionsInDateRange", "CoordActionsInDateRange", 1);

            long noOfMillisecondsinOneHour = 3600000;
            String date1 = DateUtils.formatDateUTC(new Date(nominalTime.getTime() - noOfMillisecondsinOneHour / 2));
            String date2 = DateUtils.formatDateUTC(new Date(nominalTime.getTime() + noOfMillisecondsinOneHour));
            int noOfActions = coordActionsInDateRangeXCommandObj.getCoordActionsFromDates(job.getId().toString(),
                    date1 + "::" + date2).size();
            assertEquals(1, noOfActions);

            date1 = DateUtils.formatDateUTC(new Date(nominalTime.getTime() + noOfMillisecondsinOneHour / 2));
            date2 = DateUtils.formatDateUTC(new Date(nominalTime.getTime() + noOfMillisecondsinOneHour));
            noOfActions = coordActionsInDateRangeXCommandObj.getCoordActionsFromDates(job.getId().toString(),
                    date1 + "::" + date2).size();
            assertEquals(0, noOfActions);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
