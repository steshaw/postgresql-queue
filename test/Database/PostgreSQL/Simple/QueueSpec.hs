{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Database.PostgreSQL.Simple.QueueSpec
  ( spec
  ) where

import Test.Hspec (Spec, it)
import Test.Hspec.Expectations.Lifted
import Test.Hspec.DB

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.Aeson
import Data.Either
import Data.Function
import Data.List
import Data.List.Split
import Data.Time
import Database.PostgreSQL.Simple.Queue
import Database.PostgreSQL.Simple.Queue.Migrate

queueName :: String
queueName = "q123456789012345678901234567890123456789012345678901234567890"

spec :: Spec
spec = describeDB migrate "Database.Queue" $ do
  itDB "empty locks nothing" $ do
    (either throwM pure =<< (withJobDB queueName 0 pure))
      `shouldReturn` Nothing

  itDB "empty gives count 0" $ do
    getEnqueuedCountDB queueName `shouldReturn` 0
    getFailedCountDB queueName `shouldReturn` 0

  itDB "different queues do not interfere" $ do
    let q1 = "anotherQueue"
    getEnqueuedCountDB queueName `shouldReturn` 0
    getEnqueuedCountDB q1 `shouldReturn` 0
    (either throwM pure =<< (withJobDB queueName 0 pure))
      `shouldReturn` Nothing
    void $ enqueueDB q1 $ String "foo"
    getEnqueuedCountDB queueName `shouldReturn` 0
    getEnqueuedCountDB q1 `shouldReturn` 1
    (either throwM pure =<< (withJobDB queueName 0 pure))
      `shouldReturn` Nothing
    either throwM pure =<< (withJobDB queueName 0 $ \(Job {..}) -> do
      qjAttempts `shouldBe` 0
      qjArgs `shouldBe` String "foo")

  itDB "enqueueAt in future will dequeue when ready" $ do
    let q2 = "futureQueue"
    (either throwM pure =<< (withJobDB q2 0 pure))
      `shouldReturn` Nothing
    now <- liftIO getCurrentTime
    let duration = 2 :: NominalDiffTime -- 2s
    let future = duration `addUTCTime` now -- 2s from now
    void $ enqueueAtDB q2 (String "future") future
    (either throwM pure =<< (withJobDB q2 0 pure))
      `shouldReturn` Nothing
    liftIO $ threadDelay $ 1 * 1000 * 1000
    (either throwM pure =<< (withJobDB q2 0 pure))
      `shouldReturn` Nothing
    liftIO $ threadDelay $ 1 * 1000 * 1000
    (either throwM pure =<< (withJobDB q2 0 pure)) >>= \(Just Job {..}) -> do
      qjAttempts `shouldBe` 0
      qjArgs `shouldBe` String "future"

  itDB "retryAt in future will dequeue when ready" $ do
    let q = "retryInFuture"
    (either throwM pure =<< (withJobDB q 0 pure))
      `shouldReturn` Nothing
    now <- liftIO getCurrentTime
    jobId <- enqueueDB q (String "future")
    -- Pretend that the job has failed to process and retry.
    let duration = 2 :: NominalDiffTime -- 2s
    let future = duration `addUTCTime` now -- 2s from now
    void $ retryAtDB jobId future
    (either throwM pure =<< (withJobDB q 0 pure))
      `shouldReturn` Nothing
    liftIO $ threadDelay $ 1 * 1000 * 1000
    (either throwM pure =<< (withJobDB q 0 pure))
      `shouldReturn` Nothing
    liftIO $ threadDelay $ 1 * 1000 * 1000
    (either throwM pure =<< (withJobDB q 0 pure)) >>= \(Just Job {..}) -> do
      qjAttempts `shouldBe` 1
      qjArgs `shouldBe` String "future"

  it "enqueuesDB/withJobDB" $ \conn -> do
    runDB conn $ do
      jobId <- enqueueDB queueName $ String "Hello"
      getEnqueuedCountDB queueName `shouldReturn` 1
      getFailedCountDB queueName `shouldReturn` 0

      either throwM pure =<< withJobDB queueName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId
        qjArgs `shouldBe` String "Hello"
        )

      -- Read committed but still 0. I don't depend on this but I want to
      -- see -- if it stays like this.
      getEnqueuedCountDB queueName `shouldReturn` 0
      getFailedCountDB queueName `shouldReturn` 0

    runDB conn $ do
      getEnqueuedCountDB queueName `shouldReturn` 0
      getFailedCountDB queueName `shouldReturn` 0

  it "enqueuesDB/withJobDB/retries" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB queueName $ String "Hello"
      getEnqueuedCountDB queueName `shouldReturn` 1
      getFailedCountDB queueName `shouldReturn` 0

      xs <- replicateM 7 $ withJobDB queueName 8 (\(Job {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM pure =<< withJobDB queueName 8 (\(Job {..}) -> do
        qjAttempts `shouldBe` 7
        qjArgs `shouldBe` String "Hello"
        )

    runDB conn $ do
      getEnqueuedCountDB queueName `shouldReturn` 0
      getFailedCountDB queueName `shouldReturn` 0

  it "enqueuesDB/withJobDB/fails" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB queueName $ String "Hello"
      getEnqueuedCountDB queueName `shouldReturn` 1
      getFailedCountDB queueName `shouldReturn` 0

      xs <- replicateM 2 $ withJobDB queueName 1 (\(Job {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

    runDB conn $ do
      getEnqueuedCountDB queueName `shouldReturn` 0
      getFailedCountDB queueName `shouldReturn` 1

  it "selects the oldest first" $ \conn -> do
    runDB conn $ do
      jobId0 <- enqueueDB queueName $ String "Hello"
      liftIO $ threadDelay 1000000

      jobId1 <- enqueueDB queueName $ String "Hi"

      getEnqueuedCountDB queueName `shouldReturn` 2

      either throwM pure =<< withJobDB queueName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId0
        qjArgs `shouldBe` String "Hello"
        )

      either throwM pure =<< withJobDB queueName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId1
        qjArgs `shouldBe` String "Hi"
        )

    runDB conn $ getEnqueuedCountDB queueName `shouldReturn` 0

  it "enqueues and dequeues concurrently withJob" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      lastCount <- either throwM pure =<< withPool' (\c -> withJob queueName c 0 $ \(Job {..}) -> do
        atomically $ do
          xs <- readTVar ref
          writeTVar ref $ qjArgs : xs
          pure $ length xs + 1
        )

      when (lastCount < elementCount) next

    forM_ (chunksOf (elementCount `div` 40) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue queueName c $ toJSON i

    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected
