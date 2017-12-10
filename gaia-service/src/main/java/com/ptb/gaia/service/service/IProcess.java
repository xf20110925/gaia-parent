package com.ptb.gaia.service.service;

import com.ptb.gaia.service.RankType;

import java.util.List;

interface IProcess {
        List<String> readAndFilterPmidFromDB(List<String> pmid, RankType rankType, int maxNum);
    }