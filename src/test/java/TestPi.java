import com.connell.pi.EstimatePiJob;
import org.junit.Assert;
import org.junit.Test;

public class TestPi {

  @Test
  public void testLog2_2d() {
    Assert.assertEquals(0.545177444, EstimatePiJob.EstimatePiMapper.log2_2d(3), 0.0001);
  }

  @Test
  public void testPi16d() {
    Assert.assertEquals(0.265482457, EstimatePiJob.EstimatePiMapper.pi16d(1), 0.0001);
    Assert.assertEquals(0.423429797, EstimatePiJob.EstimatePiMapper.pi16d(1000000), 0.0001);
  }

  @Test
  public void testPi16d_big() {
    Assert.assertEquals(0.265482457, EstimatePiJob.EstimatePiMapper.pi16d_big(1).doubleValue(), 0.0001);
    Assert.assertEquals(0.423429797, EstimatePiJob.EstimatePiMapper.pi16d_big(1000000).doubleValue(), 0.0001);
  }

  @Test
  public void testPi() {
    // 3.243F6A8885A308D31319

    byte[] at0 = {0x2, 0x4, 0x3, 0xF, 0x6, 0xA, 0x8};
    Assert.assertArrayEquals(at0,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(0)));

    byte[] at1 = {0x4, 0x3, 0xF, 0x6, 0xA, 0x8, 0x8};
    Assert.assertArrayEquals(at1,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(1)));

    byte[] at2 = {0x3, 0xF, 0x6, 0xA, 0x8, 0x8, 0x8};
    Assert.assertArrayEquals(at2,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(2)));

    byte[] at3 = {0xF, 0x6, 0xA, 0x8, 0x8, 0x8, 0x5};
    Assert.assertArrayEquals(at3,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(3)));

    byte[] at5 = {0xA, 0x8, 0x8, 0x8, 0x5, 0xA, 0x3};
    Assert.assertArrayEquals(at5,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(5)));

    byte[] at10 = {0xA, 0x3, 0x0, 0x8, 0xD, 0x3, 0x1};
    Assert.assertArrayEquals(at10,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(10)));

    byte[] at5320 = {0x7, 0x6, 0x3, 0xb, 0xd, 0x6, 0xe};
    Assert.assertArrayEquals(at5320,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(5320)));

    byte[] at5350 = {0x9, 0x7, 0xF, 0x4, 0x2, 0xE, 0x3};
    Assert.assertArrayEquals(at5350,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(5350)));

    byte[] at5390 = {0x1, 0xC, 0x6, 0xA, 0x1, 0x2, 0x4};
    Assert.assertArrayEquals(at5390,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(5390)));

    byte[] at9992 = {0xA, 0xA, 0xB, 0x4, 0x9, 0xE, 0xC};
    Assert.assertArrayEquals(at9992,
            EstimatePiJob.EstimatePiMapper.hexDigits(EstimatePiJob.EstimatePiMapper.pi16d(9992)));

  }

}
