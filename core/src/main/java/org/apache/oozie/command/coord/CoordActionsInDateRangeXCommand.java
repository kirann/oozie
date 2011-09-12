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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorActionInfo;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.XCommand;
import org.apache.oozie.executor.jpa.CoordJobGetActionsForDatesJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ParamChecker;

public class CoordActionsInDateRangeXCommand extends XCommand<CoordinatorActionInfo> {

    private JPAService jpaService = null;

    public CoordActionsInDateRangeXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    @Override
    protected CoordinatorActionInfo execute() throws CommandException {
        return null;
    }

    @Override
    protected String getEntityKey() {
        return null;
    }

    @Override
    protected boolean isLockRequired() {
        return false;
    }

    @Override
    protected void loadState() throws CommandException {
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }

    /**
     * Get the list of actions for given date ranges
     *
     * @param jobId coordinator job id
     * @param scope the date range for log separated by ","
     * @return the list of actions for the date range
     * @throws CommandException thrown if failed to get coordinator actions by given date range
     */
    public List<CoordinatorActionBean> getCoordActionsFromDates(String jobId, String scope) throws CommandException {
        ParamChecker.notEmpty(jobId, "jobId");
        ParamChecker.notEmpty(scope, "scope");
        Set<CoordinatorActionBean> actionSet = new HashSet<CoordinatorActionBean>();
        String[] list = scope.split(",");
        for (String s : list) {
            s = s.trim();
            if (s.contains("::")) {
                String[] dateRange = s.split("::");
                if (dateRange.length != 2) {
                    throw new CommandException(ErrorCode.E0302, "format is wrong for date's range '" + s + "'");
                }
                Date start;
                Date end;
                try {
                    start = DateUtils.parseDateUTC(dateRange[0].trim());
                    end = DateUtils.parseDateUTC(dateRange[1].trim());
                    if (start.after(end)) {
                        throw new CommandException(ErrorCode.E0302, "start date is older than end date: '" + s + "'");
                    }
                }
                catch (Exception e) {
                    throw new CommandException(ErrorCode.E0302, e);
                }
                List<CoordinatorActionBean> listOfActions = getActionIdsFromDateRange(jobId, start, end);
                actionSet.addAll(listOfActions);
            }
            else {
                throw new CommandException(ErrorCode.E0302, "format is wrong for date's range '" + s + "'");
            }
        }
        List<CoordinatorActionBean> coordActions = new ArrayList<CoordinatorActionBean>();
        for (CoordinatorActionBean coordAction : actionSet) {
            coordActions.add(coordAction);
        }
        return coordActions;
    }

    /**
     * Get coordinator action ids between given start and end time
     *
     * @param jobId coordinator job id
     * @param start start time
     * @param end end time
     * @return a list of coordinator actions that correspond to the date range
     * @throws CommandException thrown if failed to get coordinator actions
     */
    private List<CoordinatorActionBean> getActionIdsFromDateRange(String jobId, Date start, Date end)
            throws CommandException {
        List<CoordinatorActionBean> list;
        jpaService = Services.get().get(JPAService.class);
        try {
            list = jpaService.execute(new CoordJobGetActionsForDatesJPAExecutor(jobId, start, end));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        return list;
    }
}
