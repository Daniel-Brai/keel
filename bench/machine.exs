defmodule Bench.Machine do
  @spec describe(Path.t()) :: [{String.t(), String.t()}]
  def describe(dir) do
    mount = mount_of(dir)
    source = field(mount, 0)
    device = base_device(source)

    [
      {"CPU", cpu()},
      {"Memory", memory()},
      {"Disk", disk(device)},
      {"Disk power-loss protection", "not known"},
      {"Filesystem", "#{field(mount, 1)} on #{source}, options #{field(mount, 2)}"},
      {"OS", kernel()},
      {"BEAM", beam()},
      {"Keel", keel()}
    ]
  end

  defp cpu do
    {text, _status} = System.cmd("lscpu", [])
    model = line(text, "Model name")
    governor = read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")

    "#{model}, #{System.schedulers_online()} logical cores, governor #{governor}"
  end

  defp memory do
    meminfo = File.read!("/proc/meminfo")
    [_all, kib] = Regex.run(~r/MemTotal:\s+(\d+) kB/, meminfo)
    gib = String.to_integer(kib) / 1_048_576

    "#{:erlang.float_to_binary(gib, decimals: 1)} GiB"
  end

  defp disk(device) do
    {text, _status} = System.cmd("lsblk", ["-dno", "MODEL,SIZE,TRAN", "/dev/#{device}"])

    text |> String.split() |> Enum.join(" ")
  end

  defp kernel do
    {text, _status} = System.cmd("uname", ["-sr"])

    String.trim(text)
  end

  defp beam do
    otp = :erlang.system_info(:otp_release)
    schedulers = :erlang.system_info(:schedulers_online)

    "OTP #{otp}, Elixir #{System.version()}, #{schedulers} schedulers, +sbwt at its default"
  end

  defp keel do
    version = Mix.Project.config()[:version]
    {commit, 0} = System.cmd("git", ["rev-parse", "--short", "HEAD"])
    {changes, 0} = System.cmd("git", ["status", "--porcelain", "--untracked-files=no"])

    "v#{version} at commit #{String.trim(commit)}#{uncommitted(changes)}"
  end

  defp uncommitted(""), do: ""
  defp uncommitted(_changes), do: " with uncommitted changes"

  defp mount_of(dir) do
    args = ["-no", "SOURCE,FSTYPE,OPTIONS", "-T", dir]
    {text, 0} = System.cmd("findmnt", args)

    text |> String.trim() |> String.split(" ", parts: 3)
  end

  defp field(parts, index), do: Enum.at(parts, index)

  defp base_device(source) do
    plain = String.replace(source, ~r/\[.*\]$/, "")
    name = Path.basename(plain)

    String.replace(name, ~r/p?\d+$/, "")
  end

  defp line(text, key) do
    text
    |> String.split("\n")
    |> Enum.find("", &String.starts_with?(&1, key))
    |> String.split(":", parts: 2)
    |> List.last()
    |> String.trim()
  end

  defp read(path) do
    case File.read(path) do
      {:ok, text} -> String.trim(text)
      {:error, _reason} -> "unknown"
    end
  end
end
