package com.ptb.gaia.tool.esTool.data;

import com.ptb.gaia.service.entity.article.GWbArticle;

/**
 * Created by watson zhang on 2016/10/28.
 */
public class CGWbArticle extends GWbArticle{

    private double cpZpzNum;     //计算之后的值,使用e为底的对数。

    public double getCpZpzNum() {
        return cpZpzNum;
    }

    public void setCpZpzNum(double cpZpzNum) {
        this.cpZpzNum = cpZpzNum;
    }
}
