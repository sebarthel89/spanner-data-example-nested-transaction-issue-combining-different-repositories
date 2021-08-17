/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.data.spanner.core.admin.SpannerDatabaseAdminTemplate;
import org.springframework.cloud.gcp.data.spanner.core.admin.SpannerSchemaUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Example repository usage.
 *
 * @author Chengyuan Zhao
 * @author Balint Pato
 * @author Mike Eltsufin
 */
@Component
public class SpannerRepositoryExample {

  @Autowired
  private TraderRepository traderRepository;

  @Autowired
  private TradeRepository tradeRepository;

  @Autowired
  private SpannerSchemaUtils spannerSchemaUtils;

  @Autowired
  private SpannerDatabaseAdminTemplate spannerDatabaseAdminTemplate;

  void createTablesIfNotExists() {
    if (!this.spannerDatabaseAdminTemplate.tableExists("trades")) {
      this.spannerDatabaseAdminTemplate.executeDdlStrings(
          Arrays.asList(
              this.spannerSchemaUtils.getCreateTableDdlString(Trade.class)),
          true);
    }

    if (!this.spannerDatabaseAdminTemplate.tableExists("traders")) {
      this.spannerDatabaseAdminTemplate.executeDdlStrings(Arrays.asList(
          this.spannerSchemaUtils.getCreateTableDdlString(Trader.class)), true);
    }
  }

  @Transactional
  void runReadWriteTransactionExample() {
    createTablesIfNotExists();
    this.traderRepository.deleteAll();
    this.tradeRepository.deleteAll();


    // Open a single transaction
    this.traderRepository.performReadWriteTransaction(transactionTraderRepository -> {

      // save a trader
      transactionTraderRepository.save(new Trader("demo_trader1", "John", "Doe"));

      // save a trade
      // this causes a com.google.cloud.spanner.SpannerException: INTERNAL: Nested transactions are not supported
      this.tradeRepository
          .save(new Trade("1", "BUY", 100.0, 50.0, "STOCK1", "demo_trader1",
              Arrays.asList(99.0, 101.00)));

      return null;
    });
  }
}
