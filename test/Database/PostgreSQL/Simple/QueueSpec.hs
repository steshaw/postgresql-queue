{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Database.PostgreSQL.Simple.QueueSpec
  ( spec
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Control.Monad
import Data.Aeson
import Data.Function
import Data.List
import Database.PostgreSQL.Simple.Queue
import Database.PostgreSQL.Simple.Queue.Migrate
import Test.Hspec (Spec, it)
import Test.Hspec.Expectations.Lifted
import Test.Hspec.DB
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.List.Split
import Data.Either

schemaName :: String
schemaName = "complicated_name"

spec :: Spec
spec = describeDB migrate "Database.Queue" $ do
  itDB "empty locks nothing" $
    (either throwM return =<< (withJobDB schemaName 8 return))
      `shouldReturn` Nothing

  itDB "empty gives count 0" $ do
    getEnqueuedCountDB schemaName `shouldReturn` 0
    getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withJobDB" $ \conn -> do
    runDB conn $ do
      jobId <- enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      either throwM return =<< withJobDB schemaName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId
        qjArgs `shouldBe` String "Hello"
        )

      -- read committed but still 0. I don't depend on this but I want to see if it
      -- stays like this.
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withJobDB/retries" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      xs <- replicateM 7 $ withJobDB schemaName 8 (\(Job {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM return =<< withJobDB schemaName 8 (\(Job {..}) -> do
        qjAttempts `shouldBe` 7
        qjArgs `shouldBe` String "Hello"
        )

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withJobDB/fails" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      xs <- replicateM 2 $ withJobDB schemaName 1 (\(Job {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 1

  it "selects the oldest first" $ \conn -> do
    runDB conn $ do
      jobId0 <- enqueueDB schemaName $ String "Hello"
      liftIO $ threadDelay 1000000

      jobId1 <- enqueueDB schemaName $ String "Hi"

      getEnqueuedCountDB schemaName `shouldReturn` 2

      either throwM return =<< withJobDB schemaName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId0
        qjArgs `shouldBe` String "Hello"
        )

      either throwM return =<< withJobDB schemaName 8 (\(Job {..}) -> do
        qjId `shouldBe` jobId1
        qjArgs `shouldBe` String "Hi"
        )

    runDB conn $ getEnqueuedCountDB schemaName `shouldReturn` 0

  it "enqueues and dequeues concurrently withJob" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      lastCount <- either throwM return =<< withPool' (\c -> withJob schemaName c 0 $ \(Job {..}) -> do
        atomically $ do
          xs <- readTVar ref
          writeTVar ref $ qjArgs : xs
          return $ length xs + 1
        )

      when (lastCount < elementCount) next

    forM_ (chunksOf (elementCount `div` 40) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue schemaName c $ toJSON i


    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected

  --  threadDelay maxBound
