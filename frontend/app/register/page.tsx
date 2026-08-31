import { FAAuthExperience } from "@/components/fa-auth-experience";
import { safeNextPath } from "@/lib/routes";

export default async function RegisterPage({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const nextPath = safeNextPath((await searchParams).next);
  return <FAAuthExperience initialView="signup" nextPath={nextPath} />;
}
