package com.ptb.gaia.tool.esTool.data;

import com.ptb.gaia.service.entity.article.GWxArticle;

/**
 * Created by watson zhang on 2016/10/28.
 */
public class CGWxArticle extends GWxArticle{

    private double cpReadNum;        //计算之后的值,使用e为底的对数。

    public double getCpReadNum() {
        return cpReadNum;
    }

    public void setCpReadNum(double cpReadNum) {
        this.cpReadNum = cpReadNum;
    }
}
