{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Model.Session (tests) where

import           Database.LSMTree.Model.Session (Err (..))
import           GHC.Generics (Generic)
import           Test.QuickCheck
import           Test.QuickCheck.Classes (eqLaws, substitutiveEqLaws)
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.QC

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Model.Session" [
      testClassLawsWith "Err" (eqLaws (Proxy @Err)) $ \name prop ->
        testProperty name $
          (if name == "Transitive" then expectFailure else id) $
          prop
    , testClassLawsWith "Err" (substitutiveEqLaws (Proxy @Err)) $ \name prop ->
        testProperty name $ expectFailure prop
    ]

instance Arbitrary Err where
  arbitrary = oneof [
        pure ErrTableClosed
      , pure ErrSnapshotCorrupted
      , pure ErrSnapshotExists
      , pure ErrSnapshotDoesNotExist
      , pure ErrSnapshotWrongType
      , pure ErrBlobRefInvalidated
      , pure ErrCursorClosed
      , ErrDiskFault <$> arbitrary
      , ErrOther <$> arbitrary
      ]
    where
      _coveredAllCases x = case x of
          ErrTableClosed{}          -> ()
          ErrSnapshotCorrupted{}    -> ()
          ErrSnapshotExists{}       -> ()
          ErrSnapshotDoesNotExist{} -> ()
          ErrSnapshotWrongType{}    -> ()
          ErrBlobRefInvalidated{}   -> ()
          ErrCursorClosed{}         -> ()
          ErrDiskFault{}            -> ()
          ErrOther{}                -> ()

  shrink (ErrDiskFault s) = ErrDiskFault <$> shrink s
  shrink (ErrOther s)     = ErrOther <$> shrink s
  shrink _                = []

deriving stock instance Generic Err
deriving anyclass instance Function Err
deriving anyclass instance CoArbitrary Err
