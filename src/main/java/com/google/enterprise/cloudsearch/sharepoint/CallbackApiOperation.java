package com.google.enterprise.cloudsearch.sharepoint;

import com.google.api.client.json.GenericJson;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

public class CallbackApiOperation implements ApiOperation {

  private final ApiOperation delegate;
  private final SettableFuture<List<GenericJson>> result;

  public CallbackApiOperation(ApiOperation delegate) {
    this.delegate = checkNotNull(delegate, "delegated operation can not be null");
    this.result = SettableFuture.create();
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    try {
      List<GenericJson> operationResult = delegate.execute(service);
      result.set(operationResult);
      return operationResult;
    } catch (IOException io) {
      result.setException(io);
      // rethrow original error
      throw io;
    } catch (InterruptedException interrupted) {
      result.setException(interrupted);
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  @Override
  public List<GenericJson> execute(
      IndexingService service, Optional<Consumer<ApiOperation>> operationModifier)
      throws IOException, InterruptedException {
    try {
      List<GenericJson> operationResult = delegate.execute(service, operationModifier);
      result.set(operationResult);
      return operationResult;
    } catch (IOException io) {
      result.setException(io);
      // rethrow original error
      throw io;
    } catch (InterruptedException interrupted) {
      result.setException(interrupted);
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  public ListenableFuture<List<GenericJson>> getOperationResult() {
    return result;
  }
}